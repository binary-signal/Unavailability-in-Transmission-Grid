class EntsoeApiExcetpion(Exception):
    pass


class EntsoeApiUnkownMethod(EntsoeApiExcetpion):
    pass


class EntsoeApiBadParams(EntsoeApiExcetpion):
    pass


class EntsoeApiPOSTMethodMissingData(EntsoeApiExcetpion):
    pass
