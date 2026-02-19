class BasePreprocessor:
    name = ""
    merge_key = ""

    def validate(self, df):
        raise NotImplementedError

    def preprocess(self, df):
        raise NotImplementedError