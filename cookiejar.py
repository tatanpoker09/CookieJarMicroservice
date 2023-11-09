class CookieOverdrawException(Exception):
    pass


class CookieJar:
    def __init__(self, count):
        self.cookie_count = count

    def add(self, count):
        self.cookie_count += count

    def take(self, count):
        self.cookie_count -= count
        if self.cookie_count < 0:
            raise CookieOverdrawException(
                f"Cookie count has gone under 0. Current count: {self.cookie_count}"
            )

    def count(self):
        return self.cookie_count
