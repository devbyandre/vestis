"""Headline sentiment: a small finance-tuned lexicon scorer (no ML, no extra deps).

Scores a headline (optionally with its abstract) in [-1, 1]. It is
deliberately simple and transparent: count weighted positive/negative cues,
flip a cue that follows a negation, soften questions. It never reads the full
article, so treat it as a rough mood indicator.
"""
import math
import re
from typing import Optional, Tuple

POSITIVE_THRESHOLD = 0.2

_POS = """
beat surge soar jump rally rise climb gain upgrade outperform strong strength
growth grow boost profit profitable bullish optimistic win approve approval
breakthrough expand rebound recover exceed momentum upbeat buyback
raise lift accelerate improve improvement impressive robust resilient upside solid secure award surpass
""".split()

_NEG = """
miss plunge plummet tumble slump drop fall slide sink decline loss lose
downgrade underperform weak weakness warn warning cut lawsuit sue probe
investigation fraud scandal recall layoff bankruptcy default crash selloff
bearish concern fear slash halt ban fined penalty downturn crisis delay
struggle trouble shortfall dip falter worst fail disappoint disappointing
pessimistic recession bubble slip shrink drawdown downside
""".split()

# Phrases outrank single words and are removed before word matching.
_PHRASES = {
    r"(?:down|off|below)\b[^.]{0,25}(?:all[- ]time|52[- ]week) highs?": -1.0,
    r"(?:raises?|lifts?|boosts?|hikes?) (?:\w+ ){0,2}price targets?": 1.5,
    r"(?:lowers?|cuts?|slashes|trims?) (?:\w+ ){0,2}price targets?": -1.5,
    r"all[- ]time highs?": 1.5,
    r"record (?:high|profit|revenue|earnings|sales)s?": 1.5,
    r"record lows?": -1.5,
    r"beats? (?:estimates|expectations|forecasts)": 1.5,
    r"tops? (?:estimates|expectations|forecasts)": 1.5,
    r"price target (?:raised|lifted|hiked|boosted)": 1.5,
    r"misses? (?:estimates|expectations|forecasts)": -1.5,
    r"price target (?:cut|lowered|slashed)": -1.5,
    r"sell[- ]?off": -1.0,
    r"52[- ]week low": -1.0,
    r"profit warning": -1.5,
}
_PHRASE_RE = [(re.compile(p, re.I), w) for p, w in _PHRASES.items()]

_NEGATORS = {"not", "no", "never", "without", "neither", "nor", "cannot"}
_NEGATION_WINDOW = 3
_TOKEN_RE = re.compile(r"[a-z']+")


def _forms(word: str):
    stem = word[:-1] if word.endswith("e") else word
    forms = {word, word + "s", word + "es", word + "d", word + "ed", word + "ing",
             stem + "ed", stem + "ing", stem + "es"}
    if word.endswith("y"):
        forms |= {word[:-1] + "ies", word[:-1] + "ied"}
    if len(word) >= 3 and word[-1] not in "aeiouwy" and word[-2] in "aeiou" and word[-3] not in "aeiou":
        forms |= {word + word[-1] + "ed", word + word[-1] + "ing"}
    return forms


_POS_FORMS = {f for w in _POS for f in _forms(w)} | {"rose", "risen"}
_NEG_FORMS = {f for w in _NEG for f in _forms(w)} | {"fell", "fallen", "sank", "sunk", "slid", "lost", "shrank", "shrunk"}
_POS_FORMS -= _NEG_FORMS


def _cues(text: str) -> Tuple[float, float]:
    """(positive, negative) cue weights in a piece of text."""
    s = text.lower().replace("’", "'")
    pos = neg = 0.0
    for rx, weight in _PHRASE_RE:
        hits = len(rx.findall(s))
        if hits:
            if weight > 0:
                pos += weight * hits
            else:
                neg += -weight * hits
            s = rx.sub(" ", s)

    negate_left = 0
    for tok in _TOKEN_RE.findall(s):
        if tok in _NEGATORS or tok.endswith("n't"):
            negate_left = _NEGATION_WINDOW
            continue
        sign = 1 if tok in _POS_FORMS else -1 if tok in _NEG_FORMS else 0
        if sign:
            if negate_left:
                sign = -sign
            if sign > 0:
                pos += 1
            else:
                neg += 1
            negate_left = 0
        elif negate_left:
            negate_left -= 1
    return pos, neg


def _finish(pos: float, neg: float, question: bool) -> Tuple[float, str]:
    score = (pos - neg) / (pos + neg + 1.0)
    if question:
        score *= 0.5
    score = round(max(-1.0, min(1.0, score)), 3)
    if math.isclose(score, 0.0, abs_tol=1e-9):
        score = 0.0
    return score, label_for(score)


def score_headline(text: str) -> Tuple[float, str]:
    """Return (score in [-1, 1], label in {'positive', 'negative', 'neutral'})."""
    if not text:
        return 0.0, "neutral"
    pos, neg = _cues(text)
    return _finish(pos, neg, text.rstrip().endswith("?"))


HEADLINE_WEIGHT = 2.0


def score_article(title: str, summary: Optional[str] = None) -> Tuple[float, str]:
    """Score a headline together with its abstract; the headline counts double.

    Without an abstract this is exactly score_headline(title).
    """
    if not summary or not summary.strip():
        return score_headline(title)
    t_pos, t_neg = _cues(title or "")
    s_pos, s_neg = _cues(summary)
    return _finish(HEADLINE_WEIGHT * t_pos + s_pos, HEADLINE_WEIGHT * t_neg + s_neg,
                   (title or "").rstrip().endswith("?"))


def label_for(score: float) -> str:
    if score is None:
        return "neutral"
    if score >= POSITIVE_THRESHOLD:
        return "positive"
    if score <= -POSITIVE_THRESHOLD:
        return "negative"
    return "neutral"


# ── Material events ────────────────────────────────────────────────────────
# Corporate events that move a stock, recognised in a headline. Sentiment words
# alone ("drop", "bubble") say little; these say something happened.
# key: (pattern, direction, label, urgent). Urgent events may go out instantly;
# the rest only show up in the digest.
_EVENTS = {
    "profit_warning": (r"profit warning|warns? (?:on|of) (?:lower |weak\w* )?(?:profits?|earnings|sales|revenue)",
                       -1, "profit warning", True),
    "guidance_cut":   (r"(?:cuts?|lowers?|slash(?:es)?|trims?|withdraws?|pulls?|suspends?|reduces?) "
                       r"(?:its |the |full[- ]year |annual |fy |\d{4} )*(?:guidance|outlook|forecasts?)|"
                       r"(?:guidance|outlook|forecast) (?:cut|lowered|slashed|withdrawn)", -1, "guidance cut", True),
    "guidance_raise": (r"(?:raises?|lifts?|boosts?|hikes?|ups) (?:its |the |full[- ]year |annual |fy |\d{4} )*"
                       r"(?:guidance|outlook|forecasts?)|(?:guidance|outlook|forecast) (?:raised|lifted)",
                       1, "guidance raised", True),
    "earnings_miss":  (r"miss(?:es|ed)? (?:[\w-]+ ){0,2}(?:estimates|expectations|forecasts|consensus)|"
                       r"(?:earnings|results|profit|revenue|sales) (?:fall|fell|falls) short", -1, "earnings miss", True),
    "earnings_beat":  (r"(?:beats?|tops?|topped|exceeds?|exceeded|surpass(?:es|ed)?) (?:[\w-]+ ){0,2}"
                       r"(?:estimates|expectations|forecasts|consensus)", 1, "earnings beat", True),
    "takeover":       (r"takeover|buyout|tender offer|to be acquired|acquired by|(?:bid|offer) for|merger talks|"
                       r"agrees? to (?:be )?(?:bought|sold)", 1, "takeover", True),
    "fraud":          (r"fraud|accounting (?:irregularit\w+|scandal|probe)|short[- ]seller|short report|"
                       r"restat(?:e|es|ed|ement) (?:\w+ )?(?:results|earnings|accounts)", -1, "fraud allegation", True),
    "insolvency":     (r"bankrupt\w*|insolven\w*|chapter 11|creditor protection|default(?:s|ed)? on (?:its )?(?:debt|bonds?|loans?)",
                       -1, "insolvency", True),
    "halt":           (r"trading (?:halt|suspen)\w*|halts? trading|delist\w*", -1, "trading halt", True),
    "dividend_cut":   (r"(?:cuts?|slash(?:es)?|suspends?|scraps?|omits?|halts?|eliminates?) (?:its |the )?dividend|"
                       r"dividend (?:cut|suspen\w+|scrapped)", -1, "dividend cut", True),
    "ceo_exit":       (r"\b(?:ceo|cfo|chief executive|chair(?:man|woman)?)\b.{0,40}\b(?:resign\w*|steps? down|quits?|"
                       r"ousted|fired|departs?|to leave|exits?)\b|\b(?:resign\w*|ousts?|fires?)\b.{0,20}\b(?:ceo|cfo|chief executive)\b",
                       -1, "management exit", True),
    "downgrade":      (r"downgrade[sd]?\b|cut to (?:sell|underperform|underweight|neutral|hold|equal[- ]weight)",
                       -1, "downgrade", False),
    "upgrade":        (r"upgrade[sd]?\b|raised to (?:buy|outperform|overweight)", 1, "upgrade", False),
    "legal":          (r"lawsuit|class action|\bsued\b|\bsues\b|indict\w*|antitrust|probe|investigation|subpoena",
                       -1, "legal / probe", False),
    "recall":         (r"\brecall(?:s|ed|ing)?\b", -1, "recall", False),
    "layoffs":        (r"layoffs?|job cuts|cut(?:s|ting)? [\d,]+ jobs", -1, "layoffs", False),
    "breach":         (r"data breach|cyber ?attack|ransomware|hacked", -1, "cyber incident", False),
}
_EVENT_RE = [(key, re.compile(p, re.I), d, label, urgent) for key, (p, d, label, urgent) in _EVENTS.items()]
# Speculation, denials and fund holdings reports ("Shares Acquired by X Wealth
# LLC") are not events.
_NOT_EVENT = re.compile(r"\b(?:denies|denied|rules? out|no plans|rumou?rs?|could|might|would|should you|what if|"
                        r"if|whether)\b|\b(?:shares|stake|position|holdings?) (?:acquired|bought|sold|purchased|"
                        r"raised|lowered|trimmed|boosted|cut|increased|decreased)\b|\b(?:llc|13f)\b", re.I)


def material_event(title: str) -> Optional[dict]:
    """The first material event a headline reports, as
    {"key", "label", "direction" (+1/-1), "urgent"}, or None."""
    if not title or title.rstrip().endswith("?") or _NOT_EVENT.search(title):
        return None
    for key, rx, direction, label, urgent in _EVENT_RE:
        if rx.search(title):
            return {"key": key, "label": label, "direction": direction, "urgent": urgent}
    return None
