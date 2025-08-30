"""
SQLAlchemy database models for SF Events
"""

from datetime import datetime
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, Date, ForeignKey, Table, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session, sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()

# Association tables for many-to-many relationships
event_genres = Table(
    'event_genres',
    Base.metadata,
    Column('event_id', Integer, ForeignKey('events.id'), primary_key=True),
    Column('genre_id', Integer, ForeignKey('genres.id'), primary_key=True)
)

event_promoters = Table(
    'event_promoters',
    Base.metadata,
    Column('event_id', Integer, ForeignKey('events.id'), primary_key=True),
    Column('promoter_id', Integer, ForeignKey('promoters.id'), primary_key=True)
)


class Venue(Base):
    """Venue with location information"""
    __tablename__ = 'venues'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    city = Column(String(100))
    address = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    display_name = Column(Text)  # Full geocoded display name
    is_approximate = Column(Boolean, default=False)
    is_tba = Column(Boolean, default=False)
    
    # Relationships
    events = relationship("Event", back_populates="venue")
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def __repr__(self):
        return f"<Venue(name='{self.name}', city='{self.city}')>"


class Genre(Base):
    """Music genres"""
    __tablename__ = 'genres'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    
    # Relationships
    events = relationship("Event", secondary=event_genres, back_populates="genres")
    
    def __repr__(self):
        return f"<Genre(name='{self.name}')>"


class Promoter(Base):
    """Event promoters/organizers"""
    __tablename__ = 'promoters'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    
    # Relationships
    events = relationship("Event", secondary=event_promoters, back_populates="promoters")
    
    def __repr__(self):
        return f"<Promoter(name='{self.name}')>"


class Event(Base):
    """Main event table"""
    __tablename__ = 'events'
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Basic info
    title = Column(String(500), nullable=False)
    url = Column(Text)
    hidden = Column(Boolean, default=False)
    
    # Date and time
    date = Column(Date, index=True)
    day_label = Column(String(50))  # e.g., "Fri: Aug 29"
    time_range = Column(String(100))  # e.g., "10pm-2am"
    
    # Venue relationship
    venue_id = Column(Integer, ForeignKey('venues.id'))
    venue = relationship("Venue", back_populates="events")
    
    # Pricing and age
    price = Column(String(100))
    age_restriction = Column(String(50))
    
    # Relationships
    genres = relationship("Genre", secondary=event_genres, back_populates="events")
    promoters = relationship("Promoter", secondary=event_promoters, back_populates="events")
    extra_links = relationship("EventLink", back_populates="event", cascade="all, delete-orphan")
    
    # Original data tracking
    original_json = Column(Text)  # Store original JSON for reference
    source = Column(String(50))  # e.g., "19hz", "manual"
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def __repr__(self):
        return f"<Event(title='{self.title[:50]}...', date='{self.date}')>"
    
    def to_dict(self):
        """Convert to dictionary format similar to original JSON"""
        return {
            'id': self.id,
            'title': self.title,
            'url': self.url,
            'hidden': self.hidden,
            'dateISO': self.date.isoformat() if self.date else None,
            'dayLabel': self.day_label,
            'timeRange': self.time_range,
            'venue': self.venue.name if self.venue else None,
            'city': self.venue.city if self.venue else None,
            'coordinates': {
                'lat': self.venue.latitude,
                'lon': self.venue.longitude,
                'display_name': self.venue.display_name,
                'approximate': self.venue.is_approximate
            } if self.venue and self.venue.latitude else None,
            'price': self.price,
            'age': self.age_restriction,
            'genres': [g.name for g in self.genres],
            'promoters': [p.name for p in self.promoters],
            'extraLinks': [{'text': link.text, 'href': link.href} for link in self.extra_links]
        }


class EventLink(Base):
    """Extra links for events"""
    __tablename__ = 'event_links'
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, ForeignKey('events.id'), nullable=False)
    text = Column(String(255))
    href = Column(Text)
    
    # Relationships
    event = relationship("Event", back_populates="extra_links")
    
    def __repr__(self):
        return f"<EventLink(text='{self.text}', href='{self.href[:30]}...')>"


class TBAVenueHint(Base):
    """Hints for TBA venues"""
    __tablename__ = 'tba_venue_hints'
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, ForeignKey('events.id'), nullable=False)
    hint_type = Column(String(50))  # 'promoter_history', 'neighborhood', 'title_hint'
    hint_text = Column(Text)
    confidence = Column(String(20))  # 'high', 'medium', 'low'
    
    # Relationships
    event = relationship("Event")
    
    def __repr__(self):
        return f"<TBAVenueHint(type='{self.hint_type}', text='{self.hint_text[:30]}...')>"


# Database setup
def create_database(db_path: str = "events.db"):
    """Create database and tables"""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    return engine


def get_session(engine):
    """Get database session"""
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


# Query helpers
class EventQueries:
    """Common database queries for events"""
    
    @staticmethod
    def get_events_by_date(session: Session, date: Date) -> List[Event]:
        """Get all events for a specific date"""
        return session.query(Event).filter(
            Event.date == date,
            Event.hidden == False
        ).all()
    
    @staticmethod
    def get_events_by_date_range(session: Session, start_date: Date, end_date: Date) -> List[Event]:
        """Get events within a date range"""
        return session.query(Event).filter(
            Event.date >= start_date,
            Event.date <= end_date,
            Event.hidden == False
        ).all()
    
    @staticmethod
    def get_tba_events(session: Session) -> List[Event]:
        """Get all TBA venue events"""
        return session.query(Event).join(Venue).filter(
            Venue.is_tba == True,
            Event.hidden == False
        ).all()
    
    @staticmethod
    def get_events_by_genre(session: Session, genre_name: str) -> List[Event]:
        """Get events by genre"""
        return session.query(Event).join(Event.genres).filter(
            Genre.name == genre_name,
            Event.hidden == False
        ).all()
    
    @staticmethod
    def get_events_by_venue(session: Session, venue_name: str) -> List[Event]:
        """Get events by venue"""
        return session.query(Event).join(Venue).filter(
            Venue.name == venue_name,
            Event.hidden == False
        ).all()
    
    @staticmethod
    def get_events_by_promoter(session: Session, promoter_name: str) -> List[Event]:
        """Get events by promoter"""
        return session.query(Event).join(Event.promoters).filter(
            Promoter.name == promoter_name,
            Event.hidden == False
        ).all()
    
    @staticmethod
    def search_events(session: Session, query: str) -> List[Event]:
        """Search events by title, venue, or genre"""
        search_term = f"%{query}%"
        return session.query(Event).filter(
            Event.hidden == False
        ).filter(
            (Event.title.ilike(search_term)) |
            (Event.venue.has(Venue.name.ilike(search_term))) |
            (Event.genres.any(Genre.name.ilike(search_term)))
        ).all()
    
    @staticmethod
    def get_stats(session: Session) -> dict:
        """Get database statistics"""
        total_events = session.query(Event).count()
        visible_events = session.query(Event).filter(Event.hidden == False).count()
        total_venues = session.query(Venue).count()
        tba_venues = session.query(Venue).filter(Venue.is_tba == True).count()
        total_genres = session.query(Genre).count()
        total_promoters = session.query(Promoter).count()
        
        # Get date range
        dates = session.query(Event.date).filter(Event.hidden == False).all()
        date_list = [d[0] for d in dates if d[0]]
        
        return {
            'total_events': total_events,
            'visible_events': visible_events,
            'total_venues': total_venues,
            'tba_venues': tba_venues,
            'total_genres': total_genres,
            'total_promoters': total_promoters,
            'date_range': {
                'start': min(date_list).isoformat() if date_list else None,
                'end': max(date_list).isoformat() if date_list else None
            }
        }