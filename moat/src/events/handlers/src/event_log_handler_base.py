from abc import abstractmethod
from events.models import EventDto


class EventLogHandlerBase:
    NAME: str = "base"

    @abstractmethod
    def deliver_events(self, events: list[EventDto]) -> None:
        pass
