from abc import abstractmethod
from events.models import EventDto


class EventLogHandlerBase:
    NAME: str = "base"

    @abstractmethod
    def deliver_event(self, event: EventDto) -> None:
        pass
