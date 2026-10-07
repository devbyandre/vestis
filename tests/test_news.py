"""News feed: sentiment scoring, fetch pipeline (throttling, relevance), feed + API."""
import sys

import pandas as pd
import pytest

from sentiment import score_headline
from tests.conftest import seed, seed_security


def _buy(db_path, sec, qty=5):
    seed(db_path, "INSERT INTO transactions (portfolio_id, security_id, date, type, quantity, price) "
                  "VALUES (1, ?, '2024-01-02', 'buy', ?, 10)", (sec, qty))


def _sell(db_path, sec, qty=5):
    seed(db_path, "INSERT INTO transactions (portfolio_id, security_id, date, type, quantity, price) "
                  "VALUES (1, ?, '2024-02-02', 'sell', ?, 10)", (sec, qty))


def _item(uid, title=None, hours_ago=1, sentiment=0.5):
    title = title or f"Shares rally ({uid})"
    ts = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(hours=hours_ago)
    return {"uid": uid, "title": title, "publisher": "Wire", "url": f"https://x/{uid}",
            "published_at": ts.strftime("%Y-%m-%dT%H:%M:%S"), "sentiment": sentiment}


@pytest.fixture(autouse=True)
def offline_sources(monkeypatch):
    """No network in tests: external feeds return nothing unless a test says otherwise."""
    import news_sources
    monkeypatch.setattr(news_sources, "market_feeds", lambda feeds=None: [])
    monkeypatch.setattr(news_sources, "google_news", lambda name, days=7, count=10: [])
    monkeypatch.setattr(news_sources, "page_abstract", lambda url, title="": None)
    return news_sources


@pytest.fixture
def universe(db_path):
    """A holding, a watchlist entry and a fully sold position."""
    held = seed_security(db_path, "HELD")
    _buy(db_path, held)
    watch = seed_security(db_path, "WATCH")
    sold = seed_security(db_path, "SOLD")
    _buy(db_path, sold)
    _sell(db_path, sold)
    return {"held": held, "watch": watch, "sold": sold}


@pytest.fixture
def nf(mw):
    sys.modules.pop("news_fetcher", None)
    import news_fetcher
    return news_fetcher


class TestSentiment:
    @pytest.mark.parametrize("title, label", [
        ("Nvidia beats estimates and raises guidance", "positive"),
        ("Shares plunge after profit warning", "negative"),
        ("Truist Lowers Price Target on Booking Holdings to $216 From $242, Keeps Buy Rating", "negative"),
        ("Stock hits record high", "positive"),
        ("Stock hits record low", "negative"),
        ("Company does not miss earnings", "positive"),
        ("Alphabet Stock Is Down 15% From Its All-Time High. Now Is the Time to Buy", "negative"),
        ("Apple unveils new iPhone", "neutral"),
        ("Beats on revenue but warns on margins", "neutral"),
        ("", "neutral"),
    ])
    def test_labels(self, title, label):
        assert score_headline(title)[1] == label

    def test_question_is_softened(self):
        statement, _ = score_headline("Shares surge")
        question, _ = score_headline("Shares surge?")
        assert 0 < question < statement

    def test_score_is_bounded(self):
        score, _ = score_headline("surge soar jump rally gain beat upgrade strong growth")
        assert -1 <= score <= 1


class TestRelevance:
    def test_exact_symbol_only_for_exchange_suffixed_tickers(self, nf):
        # DTE (DTE Energy, NYSE) must not count as DTE.DE (Deutsche Telekom)
        assert nf._is_relevant({"relatedTickers": ["DTE.DE"]}, "DTE.DE", "EQUITY")
        assert not nf._is_relevant({"relatedTickers": ["DTE"]}, "DTE.DE", "EQUITY")
        assert not nf._is_relevant({"relatedTickers": []}, "DTE.DE", "EQUITY")

    def test_crypto_pairs_match_on_base_currency(self, nf):
        assert nf._is_relevant({"relatedTickers": ["BTC-USD"]}, "BTC-EUR", "CRYPTOCURRENCY")
        assert not nf._is_relevant({"relatedTickers": ["BTC-USD"]}, "BTC-EUR", "EQUITY")

    def test_normalize_scores_and_rejects_incomplete_items(self, nf):
        ok = nf.normalize({"uuid": "u1", "title": "Stock surges", "providerPublishTime": 1_700_000_000,
                           "link": "https://x", "publisher": "P"})
        assert ok["published_at"] == "2023-11-14T22:13:20" and ok["sentiment"] > 0
        assert nf.normalize({"title": "No time", "uuid": "u"}) is None
        assert nf.normalize({"uuid": "u", "providerPublishTime": 1}) is None


class TestRefresh:
    def _fetch(self, calls, per_symbol):
        def fetch(symbol, name, security_type):
            calls.append(symbol)
            return per_symbol.get(symbol, ([], "none"))
        return fetch

    def test_targets_are_holdings_and_watchlist_but_not_sold_positions(self, nf, db_path, universe):
        calls = []
        nf.refresh_news(fetch=self._fetch(calls, {}), sleep=lambda s: None)
        assert sorted(calls) == ["HELD", "WATCH"]

    def test_stores_items_and_throttles_the_next_run(self, nf, mw, db_path, universe):
        calls = []
        fetch = self._fetch(calls, {"HELD": ([_item("a"), _item("b")], "ticker")})
        stats = nf.refresh_news(fetch=fetch, sleep=lambda s: None)
        assert stats["stored"] == 2 and stats["checked"] == 2
        calls.clear()

        stats = nf.refresh_news(fetch=fetch, sleep=lambda s: None)
        assert calls == [] and stats["skipped"] == 2

    def test_force_for_one_symbol_ignores_the_throttle(self, nf, db_path, universe):
        calls = []
        fetch = self._fetch(calls, {"HELD": ([_item("a")], "ticker")})
        nf.refresh_news(fetch=fetch, sleep=lambda s: None)
        calls.clear()
        nf.refresh_news(symbol="held", force=True, fetch=fetch, sleep=lambda s: None)
        assert calls == ["HELD"]

    def test_symbols_without_coverage_back_off_longer(self, nf, db_path, universe):
        calls = []
        fetch = self._fetch(calls, {})
        nf.refresh_news(fetch=fetch, sleep=lambda s: None)
        # 2h later: past the 30 min throttle, still inside the 6h empty backoff
        old = (pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
        seed(db_path, "UPDATE news_fetch_log SET fetched_at=?", (old,))
        calls.clear()
        nf.refresh_news(fetch=fetch, sleep=lambda s: None)
        assert calls == []
        older = (pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:%S")
        seed(db_path, "UPDATE news_fetch_log SET fetched_at=?", (older,))
        nf.refresh_news(fetch=fetch, sleep=lambda s: None)
        assert sorted(calls) == ["HELD", "WATCH"]

    def test_failures_are_retried_next_run_and_abort_after_three_in_a_row(self, nf, db_path):
        for t in ("A", "B", "C", "D"):
            _buy(db_path, seed_security(db_path, t))
        calls = []

        def failing(symbol, name, security_type):
            calls.append(symbol)
            raise RuntimeError("429 Too Many Requests")

        stats = nf.refresh_news(fetch=failing, sleep=lambda s: None)
        assert stats["errors"] == 3 and len(calls) == 3
        assert nf.db.get_news_fetch_log().empty          # nothing recorded -> due again

    def test_old_news_is_pruned(self, nf, db_path, universe):
        old = _item("old", hours_ago=24 * 40)
        nf.db.upsert_news(universe["held"], [old, _item("new")], "2024-01-01T00:00:00")
        stats = nf.refresh_news(fetch=self._fetch([], {}), sleep=lambda s: None)
        assert stats["pruned"] == 1
        assert nf.db.get_news("2000-01-01T00:00:00")["uid"].tolist() == ["new"]

    def test_pauses_between_requests_but_not_before_the_first(self, nf, db_path, universe):
        pauses = []
        nf.refresh_news(fetch=self._fetch([], {}), sleep=pauses.append)
        assert len(pauses) == 1

    def test_upsert_refreshes_sentiment_without_duplicating(self, nf, db_path, universe):
        nf.db.upsert_news(universe["held"], [_item("a", sentiment=0.1)], "t1")
        nf.db.upsert_news(universe["held"], [_item("a", sentiment=-0.6)], "t2")
        df = nf.db.get_news("2000-01-01T00:00:00")
        assert len(df) == 1 and df.iloc[0]["sentiment"] == -0.6


class TestFeed:
    def test_one_article_for_several_securities_shows_once_with_all_symbols(self, mw, db_path, universe):
        mw.db.upsert_news(universe["held"], [_item("shared"), _item("only-held")], "t")
        mw.db.upsert_news(universe["watch"], [_item("shared")], "t")
        feed = mw.get_news_feed()
        by_uid = {i["uid"]: i for i in feed["items"]}
        assert by_uid["shared"]["symbols"] == ["HELD", "WATCH"]
        assert len(feed["items"]) == 2
        assert feed["overall"]["count"] == 2

    def test_same_story_from_two_sources_shows_once(self, mw, db_path, universe):
        mw.db.upsert_news(universe["held"], [_item("yahoo-1", "Held Corp beats estimates"),
                                             _item("rss:abc", "Held Corp beats estimates!", hours_ago=2)], "t")
        items = mw.get_news_feed()["items"]
        assert len(items) == 1 and items[0]["uid"] == "rss:abc"   # the earliest report is kept

    def test_scope_symbol_and_window_filters(self, mw, db_path, universe):
        mw.db.upsert_news(universe["held"], [_item("h1"), _item("h-old", hours_ago=24 * 10)], "t")
        mw.db.upsert_news(universe["watch"], [_item("w1")], "t")
        assert {i["uid"] for i in mw.get_news_feed(scope="holdings")["items"]} == {"h1"}
        assert {i["uid"] for i in mw.get_news_feed(scope="watchlist")["items"]} == {"w1"}
        assert {i["uid"] for i in mw.get_news_feed(symbol="watch")["items"]} == {"w1"}
        assert {i["uid"] for i in mw.get_news_feed(days=30)["items"]} == {"h1", "h-old", "w1"}

    def test_summary_lists_every_security_in_scope_even_without_coverage(self, mw, db_path, universe):
        mw.db.upsert_news(universe["held"], [_item("p", sentiment=0.6), _item("n", sentiment=-0.5),
                                             _item("z", sentiment=0.0)], "t")
        mw.db.record_news_fetch(universe["watch"], 0, "none", "2024-05-01T10:00:00")
        summary = {s["symbol"]: s for s in mw.get_news_feed(symbol="WATCH")["summary"]}  # symbol must not narrow it
        assert set(summary) == {"HELD", "WATCH"}
        held = summary["HELD"]
        assert (held["count"], held["positive"], held["negative"], held["neutral"]) == (3, 1, 1, 1)
        assert held["avg_sentiment"] == pytest.approx(0.033, abs=1e-3)
        assert summary["WATCH"]["count"] == 0 and summary["WATCH"]["avg_sentiment"] is None
        assert summary["WATCH"]["checked_at"] == "2024-05-01T10:00:00Z"

    def test_items_are_newest_first_and_capped(self, mw, db_path, universe):
        mw.db.upsert_news(universe["held"], [_item(f"n{i}", hours_ago=i + 1) for i in range(5)], "t")
        items = mw.get_news_feed(limit=3)["items"]
        assert [i["uid"] for i in items] == ["n0", "n1", "n2"]
        assert items[0]["published_at"].endswith("Z") and items[0]["label"] == "positive"

    def test_empty_database(self, mw):
        assert mw.get_news_feed() == {"items": [], "summary": [], "overall": {
            "count": 0, "avg_sentiment": None, "positive": 0, "negative": 0, "neutral": 0}}

    def test_bad_scope_is_rejected(self, mw):
        with pytest.raises(ValueError):
            mw.get_news_feed(scope="everything")


class TestApi:
    def test_get_news_shape(self, api_client, db_path, universe):
        import middleware
        middleware.db.upsert_news(universe["held"], [_item("a", "Stock surges")], "t")
        r = api_client.get("/news", params={"scope": "holdings", "days": 7})
        assert r.status_code == 200
        body = r.json()
        assert body["items"][0]["title"] == "Stock surges" and body["items"][0]["symbols"] == ["HELD"]
        assert body["summary"][0]["symbol"] == "HELD"

    def test_validates_parameters(self, api_client):
        assert api_client.get("/news", params={"scope": "bogus"}).status_code == 422
        assert api_client.get("/news", params={"days": 0}).status_code == 422

    def test_refresh_one_symbol_runs_synchronously(self, api_client, db_path, universe, monkeypatch):
        import news_fetcher
        monkeypatch.setattr(news_fetcher, "fetch_headlines",
                            lambda s, n, t: ([_item("fresh", "Shares rally")], "ticker"))
        r = api_client.post("/news/refresh", json={"symbol": "HELD"})
        assert r.status_code == 200 and r.json()["stored"] == 1
        assert [i["uid"] for i in api_client.get("/news").json()["items"]] == ["fresh"]

    def test_refresh_all_returns_immediately(self, api_client, db_path, universe, monkeypatch):
        import news_fetcher
        monkeypatch.setattr(news_fetcher, "fetch_headlines",
                            lambda s, n, t: ([_item(f"x-{s}")], "ticker"))
        monkeypatch.setattr(news_fetcher.time, "sleep", lambda s: None)
        r = api_client.post("/news/refresh", json={})
        assert r.json() == {"started": True}
        # TestClient runs background tasks before returning, so the work is visible now
        assert {i["uid"] for i in api_client.get("/news").json()["items"]} == {"x-HELD", "x-WATCH"}

    def test_refresh_reports_a_failed_fetch_in_the_stats(self, api_client, db_path, universe, monkeypatch):
        import news_fetcher

        def boom(*a):
            raise RuntimeError("yahoo down")
        monkeypatch.setattr(news_fetcher, "fetch_headlines", boom)
        r = api_client.post("/news/refresh", json={"symbol": "HELD"})
        assert r.status_code == 200 and r.json()["errors"] == 1


class TestSources:
    RSS = b"""<?xml version="1.0"?><rss><channel>
      <item><title>Allianz beats estimates - Reuters</title><link>https://example.com/a</link>
        <guid>a</guid><pubDate>Mon, 05 Oct 2026 08:00:00 GMT</pubDate><source url="x">Reuters</source></item>
      <item><title>Evil</title><link>javascript:alert(1)</link><guid>b</guid>
        <pubDate>Mon, 05 Oct 2026 09:00:00 +0200</pubDate></item>
      <item><title>No date</title><link>https://example.com/c</link><guid>c</guid></item>
    </channel></rss>"""

    def test_parse_rss_normalises_and_drops_unsafe_links(self):
        import news_sources
        items = news_sources.parse_rss(self.RSS, default_publisher="Feed")
        assert [i["title"] for i in items] == ["Allianz beats estimates", "Evil"]
        assert items[0]["publisher"] == "Reuters" and items[0]["url"] == "https://example.com/a"
        assert items[0]["sentiment"] > 0
        assert items[1]["url"] is None and items[1]["publisher"] == "Feed"
        assert items[1]["published_at"] == "2026-10-05T07:00:00"     # converted to UTC

    @pytest.mark.parametrize("raw,clean", [
        ("Allianz SE", "Allianz"), ("Novo Nordisk A/S", "Novo Nordisk"), ("Henkel AG & Co. KGaA", "Henkel"),
        ("Alibaba Group Holding Limited", "Alibaba"), ("Bitcoin EUR", "Bitcoin"), (None, None),
    ])
    def test_clean_name(self, raw, clean):
        import news_sources
        assert news_sources.clean_name(raw) == clean

    def test_match_targets_by_name_and_us_ticker(self):
        import news_sources
        targets = [{"security_id": 1, "symbol": "ALV.DE", "clean_name": "Allianz"},
                   {"security_id": 2, "symbol": "MSFT", "clean_name": "Microsoft"},
                   {"security_id": 3, "symbol": "KO", "clean_name": "Coca-Cola"}]
        assert news_sources.match_targets("Allianz shares jump", targets) == [1]
        assert news_sources.match_targets("Why $MSFT could rally", targets) == [2]
        assert news_sources.match_targets("Knock-on effects for KO fans", targets) == []   # 2-letter ticker ignored
        assert news_sources.match_targets("Allianzen und Partner", targets) == []


class TestMarketNews:
    def test_market_headlines_are_stored_and_attached_to_named_holdings(self, nf, universe, db_path, mw,
                                                                        monkeypatch, offline_sources):
        seed(db_path, "INSERT INTO securities_cache (security_id, longName) VALUES (?, 'Held Corp')",
             (universe["held"],))
        monkeypatch.setattr(offline_sources, "market_feeds", lambda feeds=None: [
            {**_item("rss:1", "Held Corp soars on deal"), "feed": "Wire"},
            {**_item("rss:2", "Oil slips as demand cools"), "feed": "Wire"},
        ])
        stats = nf.refresh_news(fetch=lambda s, n, t: ([], "none"), sleep=lambda s: None)
        assert stats["market"] == 2 and stats["attached"] == 1
        feed = mw.get_news_feed(scope="market")
        assert [i["uid"] for i in feed["items"]] == ["rss:1", "rss:2"]
        held = mw.get_news_feed(symbol="HELD")
        assert [i["title"] for i in held["items"]] == ["Held Corp soars on deal"]



class TestAbstracts:
    def test_abstract_shifts_a_neutral_headline(self):
        from sentiment import score_article, score_headline
        title = "Allianz board meets"
        assert score_headline(title)[0] == 0.0
        assert score_article(title, "Shares fell after the insurer cut its outlook and warned on demand.")[1] == "negative"
        assert score_article(title, None) == score_headline(title)
        # the headline counts double: one positive headline cue outweighs one negative abstract cue
        assert score_article("Nvidia beats", "Some analysts see a slowdown risk.")[0] > 0

    def test_feed_descriptions_become_abstracts_but_link_only_ones_do_not(self):
        import news_sources
        body = b"""<rss><channel>
          <item><title>Oil slips</title><link>https://e.com/1</link><guid>1</guid>
            <pubDate>Mon, 05 Oct 2026 08:00:00 GMT</pubDate>
            <description>&lt;p&gt;Crude prices fell sharply as demand worries weighed on markets.&lt;/p&gt;</description></item>
          <item><title>Allianz news</title><link>https://e.com/2</link><guid>2</guid>
            <pubDate>Mon, 05 Oct 2026 08:00:00 GMT</pubDate>
            <description>&lt;a href="https://news.google.com/x"&gt;Allianz news&lt;/a&gt;</description></item>
        </channel></rss>"""
        a, b = news_sources.parse_rss(body)
        assert a["summary"] == "Crude prices fell sharply as demand worries weighed on markets."
        assert a["sentiment"] < 0
        assert b["summary"] is None

    def test_page_abstracts_fetched_once_for_new_articles_within_budget(self, nf, db, universe, offline_sources, monkeypatch):
        calls = []
        def fake_abstract(url, title=""):
            calls.append(url)
            return "Profit surged to a record as demand jumped."
        monkeypatch.setattr(offline_sources, "page_abstract", fake_abstract)
        items = [_item("a", "Held Corp update"), _item("b", "Held Corp note")]
        left = nf._add_abstracts(universe["held"], items, budget=1)
        assert left == 0 and len(calls) == 1
        assert items[0]["summary"] and items[0]["sentiment"] > 0 and "summary" not in items[1]
        db.upsert_news(universe["held"], items[:1], "t")
        calls.clear()
        nf._add_abstracts(universe["held"], [_item("a", "Held Corp update")], budget=5)
        assert calls == []                      # already stored with an abstract
