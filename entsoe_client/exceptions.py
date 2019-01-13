class EntsoeApiExcetpion(Exception):
    """
    Base exception class for Entsoe API
    """

    def __init__(self, *args, **kwargs):
        super(EntsoeApiExcetpion, self).__init__(*args, **kwargs)


class EntsoeApiUnkownMethod(EntsoeApiExcetpion):
    pass


class EntsoeApiBadParams(EntsoeApiExcetpion):
    pass


class EntsoeApiPOSTMethodMissingData(EntsoeApiExcetpion):
    pass
