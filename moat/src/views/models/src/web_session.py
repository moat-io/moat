# class WebSession:
#     @property
#     def given_name(self) -> str:
#         return self.flask_session.get("userinfo").get("given_name")
#
#     @property
#     def family_name(self) -> str:
#         return self.flask_session.get("userinfo").get("family_name")
#
#     @property
#     def full_name(self) -> str:
#         return f"{self.given_name.capitalize()} {self.family_name.capitalize()}"
#
#     @property
#     def email(self) -> str:
#         return self.flask_session.get("userinfo").get("email")
#
#     @property
#     def username(self) -> str:
#         return self.flask_session.get("userinfo").get("email").split("@")[0]
#
#     def __init__(self, flask_session):
#         self.flask_session = flask_session
