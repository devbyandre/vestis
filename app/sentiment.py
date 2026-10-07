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
