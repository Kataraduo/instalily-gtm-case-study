import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data_collection.event_scraper import EventScraper


@pytest.fixture
def event_scraper():
    """Create an EventScraper instance for testing"""
    return EventScraper()


def test_init(event_scraper):
    """Test EventScraper initialization"""
    assert event_scraper is not None
    assert hasattr(event_scraper, 'headers')
    assert 'User-Agent' in event_scraper.headers


@patch('requests.get')
def test_get_events_data(mock_get, event_scraper):
    """Test get_events_data method"""
    # Mock response for requests.get
    mock_response = MagicMock()
    mock_response.text = '''
    <html>
        <body>
            <div class="event-list">
                <div class="event">
                    <h3>Test Event 1</h3>
                    <p>Date: January 1, 2023</p>
                    <p>Location: Test Location 1</p>
                    <a href="https://testevent1.com">Website</a>
                </div>
                <div class="event">
                    <h3>Test Event 2</h3>
                    <p>Date: February 2, 2023</p>
                    <p>Location: Test Location 2</p>
                    <a href="https://testevent2.com">Website</a>
                </div>
            </div>
        </body>
    </html>
    '''
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    # Mock the _extract_events_from_html method
    with patch.object(EventScraper, '_extract_events_from_html', return_value=[
        {
            'name': 'Test Event 1',
            'url': 'https://testevent1.com',
            'date': '2023-01-01',
            'location': 'Test Location 1',
            'description': 'Test Description 1',
            'relevance_score': 0.9
        },
        {
            'name': 'Test Event 2',
            'url': 'https://testevent2.com',
            'date': '2023-02-02',
            'location': 'Test Location 2',
            'description': 'Test Description 2',
            'relevance_score': 0.8
        }
    ]):
        # Call the method
        result = event_scraper.get_events_data()
        
        # Check the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'name' in result.columns
        assert 'url' in result.columns
        assert 'date' in result.columns
        assert 'location' in result.columns
        assert 'description' in result.columns
        assert 'relevance_score' in result.columns
        assert 'Test Event 1' in result['name'].values
        assert 'Test Event 2' in result['name'].values


@patch('requests.get')
def test_get_associations_data(mock_get, event_scraper):
    """Test get_associations_data method"""
    # Mock response for requests.get
    mock_response = MagicMock()
    mock_response.text = '''
    <html>
        <body>
            <div class="association-list">
                <div class="association">
                    <h3>Test Association 1</h3>
                    <p>Description: Test Description 1</p>
                    <a href="https://testassociation1.com">Website</a>
                </div>
                <div class="association">
                    <h3>Test Association 2</h3>
                    <p>Description: Test Description 2</p>
                    <a href="https://testassociation2.com">Website</a>
                </div>
            </div>
        </body>
    </html>
    '''
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    # Mock the _extract_associations_from_html method
    with patch.object(EventScraper, '_extract_associations_from_html', return_value=[
        {
            'name': 'Test Association 1',
            'url': 'https://testassociation1.com',
            'description': 'Test Description 1',
            'relevance_score': 0.9
        },
        {
            'name': 'Test Association 2',
            'url': 'https://testassociation2.com',
            'description': 'Test Description 2',
            'relevance_score': 0.8
        }
    ]):
        # Call the method
        result = event_scraper.get_associations_data()
        
        # Check the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'name' in result.columns
        assert 'url' in result.columns
        assert 'description' in result.columns
        assert 'relevance_score' in result.columns
        assert 'Test Association 1' in result['name'].values
        assert 'Test Association 2' in result['name'].values


def test_extract_events_from_html(event_scraper):
    """Test _extract_events_from_html method"""
    # Create a test HTML
    html = '''
    <html>
        <body>
            <div class="event-list">
                <div class="event">
                    <h3>Test Event 1</h3>
                    <p>Date: January 1, 2023</p>
                    <p>Location: Test Location 1</p>
                    <a href="https://testevent1.com">Website</a>
                </div>
                <div class="event">
                    <h3>Test Event 2</h3>
                    <p>Date: February 2, 2023</p>
                    <p>Location: Test Location 2</p>
                    <a href="https://testevent2.com">Website</a>
                </div>
            </div>
        </body>
    </html>
    '''
    
    # Call the method
    with patch.object(EventScraper, '_calculate_relevance_score', return_value=0.85):
        result = event_scraper._extract_events_from_html(html)
    
    # Check the result
    assert isinstance(result, list)
    assert len(result) > 0
    # The actual extraction logic would be more complex and depend on the HTML structure
    # This is a simplified test


def test_extract_associations_from_html(event_scraper):
    """Test _extract_associations_from_html method"""
    # Create a test HTML
    html = '''
    <html>
        <body>
            <div class="association-list">
                <div class="association">
                    <h3>Test Association 1</h3>
                    <p>Description: Test Description 1</p>
                    <a href="https://testassociation1.com">Website</a>
                </div>
                <div class="association">
                    <h3>Test Association 2</h3>
                    <p>Description: Test Description 2</p>
                    <a href="https://testassociation2.com">Website</a>
                </div>
            </div>
        </body>
    </html>
    '''
    
    # Call the method
    with patch.object(EventScraper, '_calculate_relevance_score', return_value=0.85):
        result = event_scraper._extract_associations_from_html(html)
    
    # Check the result
    assert isinstance(result, list)
    assert len(result) > 0
    # The actual extraction logic would be more complex and depend on the HTML structure
    # This is a simplified test


def test_calculate_relevance_score(event_scraper):
    """Test _calculate_relevance_score method"""
    # Test with a highly relevant text
    high_relevance_text = "This is about solar panels, protective films, and building materials."
    high_score = event_scraper._calculate_relevance_score(high_relevance_text)
    
    # Test with a less relevant text
    low_relevance_text = "This is about something completely unrelated to the target industry."
    low_score = event_scraper._calculate_relevance_score(low_relevance_text)
    
    # The high relevance text should have a higher score
    assert high_score > low_score