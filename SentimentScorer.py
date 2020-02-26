from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

sia = SIA()


class SentimentScorer:
    @staticmethod
    def score(text: str) -> float:
        result = sia.polarity_scores(text)
        # Has keys neg, neu, pos, compound
        # print(result)
        return result['compound']


# scores = SentimentScorer.sentiment_scorer('Gimme that Christian side hug!')
# print(scores)
