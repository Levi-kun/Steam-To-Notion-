"""
Steam-Notion Gaming Tracker - Enhanced Version
A comprehensive tool to monitor gaming habits, rate games, track sessions,
and analyze spending patterns by connecting Steam API with Notion database.
"""

import os
import time
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gaming_tracker.log'),
        logging.StreamHandler()
    ]
)

class SteamAPI:
    """Handle Steam Web API interactions"""
    
    def __init__(self, api_key: str, steam_id: str):
        self.api_key = api_key
        self.steam_id = steam_id
        self.base_url = "http://api.steampowered.com"
        
    def get_owned_games(self) -> Dict:
        """Get list of games owned by user"""
        url = f"{self.base_url}/IPlayerService/GetOwnedGames/v0001/"
        params = {
            'key': self.api_key,
            'steamid': self.steam_id,
            'format': 'json',
            'include_appinfo': 1,
            'include_played_free_games': 1
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error fetching owned games: {e}")
            return {}
    
    def get_recently_played_games(self) -> Dict:
        """Get recently played games (last 2 weeks)"""
        url = f"{self.base_url}/IPlayerService/GetRecentlyPlayedGames/v0001/"
        params = {
            'key': self.api_key,
            'steamid': self.steam_id,
            'format': 'json'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error fetching recently played games: {e}")
            return {}
    
    def get_game_details(self, app_id: int) -> Dict:
        """Get detailed information about a specific game"""
        url = f"http://store.steampowered.com/api/appdetails"
        params = {
            'appids': app_id,
            'format': 'json'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get(str(app_id), {}).get('data', {})
        except requests.RequestException as e:
            logging.error(f"Error fetching game details for {app_id}: {e}")
            return {}
    
    def get_game_achievements(self, app_id: int) -> Dict:
        """Get game achievements for the player"""
        url = f"{self.base_url}/ISteamUserStats/GetPlayerAchievements/v0001/"
        params = {
            'key': self.api_key,
            'steamid': self.steam_id,
            'appid': app_id,
            'format': 'json'
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 403:
                # Game stats are private or don't exist
                return {}
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.debug(f"Error fetching achievements for {app_id}: {e}")
            return {}
    
    def get_game_schema(self, app_id: int) -> Dict:
        """Get achievement schema for a game"""
        url = f"{self.base_url}/ISteamUserStats/GetSchemaForGame/v2/"
        params = {
            'key': self.api_key,
            'appid': app_id,
            'format': 'json'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.debug(f"Error fetching schema for {app_id}: {e}")
            return {}
    
    def get_player_stats(self) -> Dict:
        """Get player statistics"""
        url = f"{self.base_url}/ISteamUser/GetPlayerSummaries/v0002/"
        params = {
            'key': self.api_key,
            'steamids': self.steam_id,
            'format': 'json'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error fetching player stats: {e}")
            return {}

class SessionTracker:
    """Track gaming sessions based on playtime changes"""
    
    def __init__(self):
        self.session_file = 'gaming_sessions.json'
        self.load_sessions()
    
    def load_sessions(self):
        """Load existing session data"""
        try:
            with open(self.session_file, 'r') as f:
                self.sessions = json.load(f)
        except FileNotFoundError:
            self.sessions = {}
    
    def save_sessions(self):
        """Save session data to file"""
        with open(self.session_file, 'w') as f:
            json.dump(self.sessions, f, indent=2)
    
    def update_session_count(self, app_id: int, current_playtime: int, last_played: int) -> int:
        """Update session count based on playtime changes"""
        app_id_str = str(app_id)
        
        # Initialize if not exists
        if app_id_str not in self.sessions:
            self.sessions[app_id_str] = {
                'session_count': 1 if current_playtime > 0 else 0,
                'last_playtime': current_playtime,
                'last_played': last_played
            }
        else:
            session_data = self.sessions[app_id_str]
            
            # Check if playtime increased (new session)
            if current_playtime > session_data.get('last_playtime', 0):
                # Only increment if significant time passed since last session (more than 1 hour)
                last_session_time = session_data.get('last_played', 0)
                if last_played - last_session_time > 3600:  # 1 hour in seconds
                    session_data['session_count'] += 1
                
                session_data['last_playtime'] = current_playtime
                session_data['last_played'] = last_played
        
        self.save_sessions()
        return self.sessions[app_id_str]['session_count']

class NotionAPI:
    """Handle Notion API interactions with enhanced properties"""
    
    def __init__(self, token: str, database_id: str):
        self.token = token
        self.database_id = database_id
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"  # Latest stable Notion API version
        }
        self.base_url = "https://api.notion.com/v1"
    
    def calculate_average_rating(self, ratings: Dict[str, float]) -> float:
        """Calculate average rating from individual category ratings"""
        valid_ratings = [score for score in ratings.values() if score > 0]
        return round(sum(valid_ratings) / len(valid_ratings), 1) if valid_ratings else 0
    
    def create_game_entry(self, game_data: Dict, session_count: int = 0, 
                         achievement_completion: float = 0) -> Optional[str]:
        """Create a new game entry in Notion database"""
        url = f"{self.base_url}/pages"
        
        # Calculate hours played
        playtime_minutes = game_data.get('playtime_forever', 0)
        hours_played = round(playtime_minutes / 60, 1) if playtime_minutes > 0 else 0
        
        # Calculate cost per hour if price is available
        price = game_data.get('price_overview', {}).get('final', 0) / 100 if game_data.get('price_overview') else 0
        cost_per_hour = round(price / hours_played, 2) if hours_played > 0 and price > 0 else 0
        
        # Format last played date
        last_played_date = None
        if game_data.get('rtime_last_played'):
            last_played_date = datetime.fromtimestamp(game_data.get('rtime_last_played')).isoformat()
        
        # Format release date
        release_date = None
        if game_data.get('release_date', {}).get('date'):
            try:
                # Try to parse the release date
                release_str = game_data['release_date']['date']
                # Handle various date formats from Steam
                for fmt in ['%b %d, %Y', '%b %Y', '%Y']:
                    try:
                        parsed_date = datetime.strptime(release_str, fmt)
                        release_date = parsed_date.isoformat()[:10]  # Just the date part
                        break
                    except ValueError:
                        continue
            except:
                pass
        
        properties = {
            "Game Name": {"title": [{"text": {"content": game_data.get('name', 'Unknown')}}]},
            "App ID": {"number": game_data.get('appid', 0)},
            "Hours Played": {"number": hours_played},
            "Session Count": {"number": session_count},
            "Last Played": {"date": {"start": last_played_date} if last_played_date else None},
            "Most Recent Session": {"date": {"start": last_played_date} if last_played_date else None},
            "Genres": {
                "multi_select": [
                    {"name": genre.get('description', '')} 
                    for genre in game_data.get('genres', [])[:5]  # Limit to 5 genres
                ]
            },
            "Price": {"number": price},
            "Cost Per Hour": {"number": cost_per_hour},
            "Release Date": {"date": {"start": release_date} if release_date else None},
            "Purchase Date": {"date": None},  # To be filled manually
            "Developer": {
                "rich_text": [
                    {"text": {"content": ', '.join(game_data.get('developers', []))}}
                ]
            },
            "Publisher": {
                "rich_text": [
                    {"text": {"content": ', '.join(game_data.get('publishers', []))}}
                ]
            },
            "Achievement Completion": {"number": achievement_completion},
            
            # Rating categories (0-10)
            "Gameplay Rating": {"number": 0},
            "Story/Worldbuilding Rating": {"number": 0},
            "Graphics/Art Style Rating": {"number": 0},
            "Music/Sound Design Rating": {"number": 0},
            "Replayability Rating": {"number": 0},
            "Emotional Impact Rating": {"number": 0},
            # Overall Rating will be calculated by Notion formula, not set here
            
            "Notes": {"rich_text": []},
            "Status": {"select": {"name": "Owned"}},
            "Platform": {"multi_select": [{"name": "Steam"}]},
            
            # Game description
            "Description": {
                "rich_text": [
                    {"text": {"content": game_data.get('short_description', '')[:2000]}}  # Notion has char limits
                ]
            }
        }
        
        payload = {
            "parent": {"database_id": self.database_id},
            "properties": properties
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            logging.info(f"Created entry for: {game_data.get('name')}")
            return response.json().get('id')
        except requests.RequestException as e:
            logging.error(f"Error creating Notion entry for {game_data.get('name')}: {e}")
            if hasattr(e, 'response'):
                logging.error(f"Response: {e.response.text}")
            return None
    
    def update_game_entry(self, page_id: str, game_data: Dict, session_count: int = 0,
                         achievement_completion: float = 0) -> bool:
        """Update an existing game entry"""
        url = f"{self.base_url}/pages/{page_id}"
        
        playtime_minutes = game_data.get('playtime_forever', 0)
        hours_played = round(playtime_minutes / 60, 1) if playtime_minutes > 0 else 0
        
        # Calculate cost per hour
        price = game_data.get('price_overview', {}).get('final', 0) / 100 if game_data.get('price_overview') else 0
        cost_per_hour = round(price / hours_played, 2) if hours_played > 0 and price > 0 else 0
        
        last_played_date = None
        if game_data.get('rtime_last_played'):
            last_played_date = datetime.fromtimestamp(game_data.get('rtime_last_played')).isoformat()
        
        properties = {
            "Hours Played": {"number": hours_played},
            "Session Count": {"number": session_count},
            "Last Played": {"date": {"start": last_played_date} if last_played_date else None},
            "Most Recent Session": {"date": {"start": last_played_date} if last_played_date else None},
            "Cost Per Hour": {"number": cost_per_hour},
            "Achievement Completion": {"number": achievement_completion}
        }
        
        payload = {"properties": properties}
        
        try:
            response = requests.patch(url, headers=self.headers, json=payload)
            response.raise_for_status()
            logging.info(f"Updated entry for: {game_data.get('name')}")
            return True
        except requests.RequestException as e:
            logging.error(f"Error updating Notion entry: {e}")
            return False
    
    def get_existing_games(self) -> Dict[int, str]:
        """Get existing games from Notion database"""
        url = f"{self.base_url}/databases/{self.database_id}/query"
        
        existing_games = {}
        has_more = True
        start_cursor = None
        
        while has_more:
            payload = {"page_size": 100}
            if start_cursor:
                payload["start_cursor"] = start_cursor
            
            try:
                response = requests.post(url, headers=self.headers, json=payload)
                response.raise_for_status()
                data = response.json()
                
                for page in data.get('results', []):
                    app_id_prop = page.get('properties', {}).get('App ID', {})
                    if app_id_prop.get('number'):
                        existing_games[app_id_prop['number']] = page['id']
                
                has_more = data.get('has_more', False)
                start_cursor = data.get('next_cursor')
                
            except requests.RequestException as e:
                logging.error(f"Error fetching existing games: {e}")
                break
        
        return existing_games
    
    def create_database_schema(self) -> bool:
        """Display the enhanced database schema"""
        schema_info = """
        Enhanced Notion Database Schema for Gaming Tracker:
        
        Properties to create in your Notion database:
        
        BASIC INFO:
        1. Game Name (Title)
        2. App ID (Number)
        3. Hours Played (Number)
        4. Session Count (Number) - NEW
        5. Last Played (Date)
        6. Most Recent Session (Date) - NEW
        7. Purchase Date (Date) - NEW (Manual input)
        
        GAME DETAILS:
        8. Genres (Multi-select)
        9. Price (Number)
        10. Cost Per Hour (Number)
        11. Release Date (Date)
        12. Developer (Text)
        13. Publisher (Text)
        14. Description (Text) - NEW
        15. Achievement Completion (Number) - NEW (0-100%)
        
        RATING SYSTEM (All 0-10):
        16. Gameplay Rating (Number) - NEW
        17. Story/Worldbuilding Rating (Number) - NEW
        18. Graphics/Art Style Rating (Number) - NEW
        19. Music/Sound Design Rating (Number) - NEW
        20. Replayability Rating (Number) - NEW
        21. Emotional Impact Rating (Number) - NEW
        22. Overall Rating (Formula) - NEW (Use Notion formula for average)
        
        STATUS & NOTES:
        23. Notes (Text)
        24. Status (Select: Owned, Completed, Playing, Wishlist, Dropped, On Hold)
        25. Platform (Multi-select: Steam, Epic, GOG, etc.)
        
        IMPORTANT: Use Notion's native Formula property for Overall Rating instead of letting Python calculate it.
        This allows for real-time updates when you manually change individual ratings.
        
        Notion Formula for Overall Rating:
        round(if(prop("Gameplay Rating") > 0 or prop("Story/Worldbuilding Rating") > 0 or prop("Graphics/Art Style Rating") > 0 or prop("Music/Sound Design Rating") > 0 or prop("Replayability Rating") > 0 or prop("Emotional Impact Rating") > 0, (if(prop("Gameplay Rating") > 0, prop("Gameplay Rating"), 0) + if(prop("Story/Worldbuilding Rating") > 0, prop("Story/Worldbuilding Rating"), 0) + if(prop("Graphics/Art Style Rating") > 0, prop("Graphics/Art Style Rating"), 0) + if(prop("Music/Sound Design Rating") > 0, prop("Music/Sound Design Rating"), 0) + if(prop("Replayability Rating") > 0, prop("Replayability Rating"), 0) + if(prop("Emotional Impact Rating") > 0, prop("Emotional Impact Rating"), 0)) / (if(prop("Gameplay Rating") > 0, 1, 0) + if(prop("Story/Worldbuilding Rating") > 0, 1, 0) + if(prop("Graphics/Art Style Rating") > 0, 1, 0) + if(prop("Music/Sound Design Rating") > 0, 1, 0) + if(prop("Replayability Rating") > 0, 1, 0) + if(prop("Emotional Impact Rating") > 0, 1, 0)), 0), 1)
        """
        print(schema_info)
        return True

class GameAnalyzer:
    """Enhanced game analysis with session and rating data"""
    
    @staticmethod
    def analyze_playtime_distribution(games: List[Dict]) -> Dict:
        """Analyze playtime distribution across games"""
        total_playtime = sum(game.get('playtime_forever', 0) for game in games)
        total_hours = total_playtime / 60
        
        # Categorize games by playtime
        categories = {
            'unplayed': [],
            'light_play': [],    # < 5 hours
            'moderate_play': [], # 5-20 hours
            'heavy_play': [],    # 20-100 hours
            'excessive_play': [] # > 100 hours
        }
        
        for game in games:
            hours = game.get('playtime_forever', 0) / 60
            name = game.get('name', 'Unknown')
            
            if hours == 0:
                categories['unplayed'].append(name)
            elif hours < 5:
                categories['light_play'].append((name, round(hours, 1)))
            elif hours < 20:
                categories['moderate_play'].append((name, round(hours, 1)))
            elif hours < 100:
                categories['heavy_play'].append((name, round(hours, 1)))
            else:
                categories['excessive_play'].append((name, round(hours, 1)))
        
        return {
            'total_hours': round(total_hours, 1),
            'total_games': len(games),
            'categories': categories,
            'unplayed_percentage': round(len(categories['unplayed']) / len(games) * 100, 1)
        }
    
    @staticmethod
    def analyze_genres(games: List[Dict]) -> Dict:
        """Analyze preferred genres"""
        genre_stats = {}
        
        for game in games:
            genres = game.get('genres', [])
            playtime = game.get('playtime_forever', 0)
            
            for genre in genres:
                genre_name = genre.get('description', 'Unknown')
                if genre_name not in genre_stats:
                    genre_stats[genre_name] = {
                        'count': 0,
                        'total_playtime': 0,
                        'games': []
                    }
                
                genre_stats[genre_name]['count'] += 1
                genre_stats[genre_name]['total_playtime'] += playtime
                genre_stats[genre_name]['games'].append(game.get('name', 'Unknown'))
        
        # Sort by total playtime
        sorted_genres = sorted(
            genre_stats.items(),
            key=lambda x: x[1]['total_playtime'],
            reverse=True
        )
        
        return dict(sorted_genres)
    
    @staticmethod
    def analyze_sessions(session_tracker: SessionTracker, games: List[Dict]) -> Dict:
        """Analyze gaming sessions"""
        session_stats = {
            'total_sessions': 0,
            'avg_sessions_per_game': 0,
            'most_sessioned_games': [],
            'session_efficiency': []  # Hours per session
        }
        
        for game in games:
            app_id = game.get('appid')
            if str(app_id) in session_tracker.sessions:
                session_count = session_tracker.sessions[str(app_id)]['session_count']
                session_stats['total_sessions'] += session_count
                
                hours_played = game.get('playtime_forever', 0) / 60
                if session_count > 0:
                    hours_per_session = round(hours_played / session_count, 1)
                    session_stats['session_efficiency'].append({
                        'name': game.get('name', 'Unknown'),
                        'sessions': session_count,
                        'hours_per_session': hours_per_session
                    })
        
        # Calculate averages
        total_games_with_sessions = len([g for g in games if str(g.get('appid', 0)) in session_tracker.sessions])
        if total_games_with_sessions > 0:
            session_stats['avg_sessions_per_game'] = round(
                session_stats['total_sessions'] / total_games_with_sessions, 1
            )
        
        # Sort by session count
        session_stats['session_efficiency'].sort(key=lambda x: x['sessions'], reverse=True)
        session_stats['most_sessioned_games'] = session_stats['session_efficiency'][:10]
        
        return session_stats

class GamingTracker:
    """Enhanced main application class"""
    
    def __init__(self, steam_api_key: str, steam_id: str, notion_token: str, notion_database_id: str):
        self.steam_api = SteamAPI(steam_api_key, steam_id)
        self.notion_api = NotionAPI(notion_token, notion_database_id)
        self.analyzer = GameAnalyzer()
        self.session_tracker = SessionTracker()
    
    def calculate_achievement_completion(self, app_id: int) -> float:
        """Calculate achievement completion percentage"""
        try:
            achievements_data = self.steam_api.get_game_achievements(app_id)
            if not achievements_data.get('playerstats', {}).get('achievements'):
                return 0
            
            achievements = achievements_data['playerstats']['achievements']
            if not achievements:
                return 0
            
            completed = sum(1 for ach in achievements if ach.get('achieved') == 1)
            total = len(achievements)
            
            return round((completed / total) * 100, 1) if total > 0 else 0
            
        except Exception as e:
            logging.debug(f"Could not calculate achievements for {app_id}: {e}")
            return 0
    
    def is_valid_game(self, game_details: Dict) -> bool:
        """Check if the app is a valid game (not DLC or software)"""
        # Check if the type is explicitly 'game'
        if game_details.get('type', '').lower() != 'game':
            logging.debug(f"Skipping {game_details.get('name', 'Unknown')} - type is {game_details.get('type', 'unknown')}")
            return False
        
        # Additional check: ensure it has genres typical of games
        genres = game_details.get('genres', [])
        if not genres:
            logging.debug(f"Skipping {game_details.get('name', 'Unknown')} - no genres found")
            return False
            
        # Check for non-game categories in genres
        non_game_genres = {'Utilities', 'Software', 'Video Production', 'Animation & Modeling', 'Design & Illustration'}
        has_game_genres = any(genre.get('description') not in non_game_genres for genre in genres)
        if not has_game_genres:
            logging.debug(f"Skipping {game_details.get('name', 'Unknown')} - only non-game genres found")
            return False
            
        return True
    
    def sync_games_to_notion(self, update_existing: bool = True, include_achievements: bool = True) -> Dict:
        """Enhanced sync with session tracking and achievements"""
        logging.info("Starting enhanced game sync to Notion...")
        
        # Get games from Steam
        owned_games_data = self.steam_api.get_owned_games()
        games = owned_games_data.get('response', {}).get('games', [])
        
        if not games:
            logging.warning("No games found in Steam library")
            return {'synced': 0, 'updated': 0, 'errors': 0, 'skipped': 0}
        
        # Get existing games from Notion
        existing_games = self.notion_api.get_existing_games() if update_existing else {}
        
        synced = updated = errors = skipped = 0
        
        for i, game in enumerate(games):
            
            app_id = game.get('appid')
            # Skip if not a valid game
                    if not self.is_valid_game(game_details):
                        logging.info(f"Skipped {game.get('name', 'Unknown')} (AppID: {app_id}) - not a game")
                        skipped += 1
                        continue
            
            try:
                # Get detailed game information
                game_details = self.steam_api.get_game_details(app_id)
                
                # Calculate session count
                session_count = self.session_tracker.update_session_count(
                    app_id,
                    game.get('playtime_forever', 0),
                    game.get('rtime_last_played', 0)
                )
                
                # Calculate achievement completion
                achievement_completion = 0
                if include_achievements:
                    achievement_completion = self.calculate_achievement_completion(app_id)
                
                # Merge basic info with detailed info
                full_game_data = {**game, **game_details}
                
                if app_id in existing_games and update_existing:
                    # Update existing entry
                    if self.notion_api.update_game_entry(
                        existing_games[app_id], 
                        full_game_data, 
                        session_count,
                        achievement_completion
                    ):
                        updated += 1
                    else:
                        errors += 1
                elif app_id not in existing_games:
                    # Create new entry
                    if self.notion_api.create_game_entry(
                        full_game_data, 
                        session_count,
                        achievement_completion
                    ):
                        synced += 1
                    else:
                        errors += 1
                
                # Progress logging
                if (i + 1) % 10 == 0:
                    logging.info(f"Processed {i + 1}/{len(games)} games")
                
                # Rate limiting to avoid hitting API limits
                time.sleep(1)  # Increased delay for stability
                
            except Exception as e:
                logging.error(f"Error processing game {game.get('name')}: {e}")
                errors += 1
        
        results = {'synced': synced, 'updated': updated, 'errors': errors}
        logging.info(f"Enhanced sync completed: {results}")
        return results
    
    def generate_enhanced_gaming_report(self) -> Dict:
        """Generate comprehensive gaming report with sessions and achievements"""
        logging.info("Generating enhanced gaming report...")
        
        valid_games = []
        for game in games:
            details = self.steam_api.get_game_details(game.get('appid'))
            if self.is_valid_game(details):
                valid_games.append({**game, **details})
            time.sleep(0.01)
            
        # Get all owned games
        owned_games_data = self.steam_api.get_owned_games()
        games = owned_games_data.get('response', {}).get('games', [])
        
        # Get recently played games
        recent_games_data = self.steam_api.get_recently_played_games()
        recent_games = recent_games_data.get('response', {}).get('games', [])
        
        # Analyze data
        playtime_analysis = self.analyzer.analyze_playtime_distribution(games)
        session_analysis = self.analyzer.analyze_sessions(self.session_tracker, games)
        
        # Get detailed info for genre analysis (sample of games to avoid rate limiting)
        sample_games = games[:30]  # Reduced sample to speed up report generation
        detailed_games = []
        
        for game in sample_games:
            details = self.steam_api.get_game_details(game.get('appid'))
            if details and self.is_valid_game(details):
                detailed_games.append({**game, **details})
            time.sleep(0.01)
            
        logging.info("Analyzing sample games for detailed insights...")
        for game in sample_games:
            details = self.steam_api.get_game_details(game.get('appid'))
            if details:
                detailed_games.append({**game, **details})
            time.sleep(0.3)  # Rate limiting
        
        genre_analysis = self.analyzer.analyze_genres(detailed_games)
        
        # Calculate achievement stats
        achievement_stats = {
            'games_with_achievements': 0,
            'total_achievements_earned': 0,
            'avg_completion_rate': 0
        }
        
        # Sample achievement calculation (limited to avoid rate limits)
        sample_for_achievements = games[:10]
        completion_rates = []
        
        for game in sample_for_achievements:
            completion = self.calculate_achievement_completion(game.get('appid'))
            if completion > 0:
                achievement_stats['games_with_achievements'] += 1
                completion_rates.append(completion)
            time.sleep(0.5)
        
        if completion_rates:
            achievement_stats['avg_completion_rate'] = round(sum(completion_rates) / len(completion_rates), 1)
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'library_stats': {
                'total_games': len(games),
                'total_hours': playtime_analysis['total_hours'],
                'recently_played': len(recent_games),
                'total_sessions': session_analysis['total_sessions'],
                'avg_sessions_per_game': session_analysis['avg_sessions_per_game']
            },
            'playtime_analysis': playtime_analysis,
            'session_analysis': session_analysis,
            'achievement_stats': achievement_stats,
            'genre_analysis': dict(list(genre_analysis.items())[:10]),  # Top 10 genres
            'recent_activity': [
                {
                    'name': game.get('name'),
                    'hours_2weeks': round(game.get('playtime_2weeks', 0) / 60, 1),
                    'total_hours': round(game.get('playtime_forever', 0) / 60, 1),
                    'sessions': self.session_tracker.sessions.get(str(game.get('appid')), {}).get('session_count', 0)
                }
                for game in recent_games[:10]  # Top 10 recent games
            ]
        }
        
        return report
    
    def save_report_to_file(self, report: Dict, filename: str = None) -> str:
        """Save report to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"gaming_report_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Report saved to {filename}")
        return filename

def main():
    """Main function to run the enhanced gaming tracker"""
    
    # Configuration - Replace with your actual API keys
    config = {
        'STEAM_API_KEY': 'your_steam_api_key_here',
        'STEAM_ID': 'your_steam_id_here',
        'NOTION_TOKEN': 'your_notion_integration_token_here',
        'NOTION_DATABASE_ID': 'your_notion_database_id_here'
    }
    
    # Check for environment variables
    for key, value in config.items():
        env_value = os.getenv(key)
        if env_value:
            config[key] = env_value
    
    # Validate configuration
    if any(value.startswith('your_') for value in config.values()):
        print("Please configure your API keys and IDs in the config section or as environment variables")
        print("Required environment variables:")
        for key in config.keys():
            print(f"  {key}")
        return
    
    # Initialize enhanced tracker
    tracker = GamingTracker(
        config['STEAM_API_KEY'],
        config['STEAM_ID'],
        config['NOTION_TOKEN'],
        config['NOTION_DATABASE_ID']
    )
    
    # Enhanced menu system
    while True:
        print("\n=== Enhanced Steam-Notion Gaming Tracker ===")
        print("1. Sync games to Notion (with sessions & achievements)")
        print("2. Generate enhanced gaming report")
        print("3. Show enhanced database schema")
        print("4. Quick sync (no achievements)")
        print("5. Update session counts only")
        print("6. View session statistics")
        print("7. Exit")
        
        choice = input("Choose an option (1-7): ").strip()
        
        if choice == '1':
            print("Syncing games to Notion with full data...")
            print("This may take a while due to achievement processing...")
            results = tracker.sync_games_to_notion(include_achievements=True)
            print(f"Full sync completed: {results}")
            
        elif choice == '2':
            print("Generating enhanced gaming report...")
            report = tracker.generate_enhanced_gaming_report()
            filename = tracker.save_report_to_file(report)
            print(f"Enhanced report generated and saved to: {filename}")
            
            # Print enhanced summary
            print(f"\n=== Enhanced Gaming Report Summary ===")
            print(f"Total Games: {report['library_stats']['total_games']}")
            print(f"Total Hours: {report['library_stats']['total_hours']}")
            print(f"Total Sessions: {report['library_stats']['total_sessions']}")
            print(f"Avg Sessions/Game: {report['library_stats']['avg_sessions_per_game']}")
            print(f"Unplayed Games: {report['playtime_analysis']['unplayed_percentage']}%")
            if report['achievement_stats']['games_with_achievements'] > 0:
                print(f"Avg Achievement Completion: {report['achievement_stats']['avg_completion_rate']}%")
            
        elif choice == '3':
            tracker.notion_api.create_database_schema()
            
        elif choice == '4':
            print("Quick syncing games (no achievements)...")
            results = tracker.sync_games_to_notion(include_achievements=False)
            print(f"Quick sync completed: {results}")
            
        elif choice == '5':
            print("Updating session counts...")
            owned_games_data = tracker.steam_api.get_owned_games()
            games = owned_games_data.get('response', {}).get('games', [])
            
            for game in games:
                tracker.session_tracker.update_session_count(
                    game.get('appid'),
                    game.get('playtime_forever', 0),
                    game.get('rtime_last_played', 0)
                )
            
            print(f"Updated session counts for {len(games)} games")
            
        elif choice == '6':
            print("Calculating session statistics...")
            owned_games_data = tracker.steam_api.get_owned_games()
            games = owned_games_data.get('response', {}).get('games', [])
            session_stats = tracker.analyzer.analyze_sessions(tracker.session_tracker, games)
            
            print(f"\n=== Session Statistics ===")
            print(f"Total Gaming Sessions: {session_stats['total_sessions']}")
            print(f"Average Sessions per Game: {session_stats['avg_sessions_per_game']}")
            print(f"\nTop Games by Session Count:")
            for game in session_stats['most_sessioned_games'][:5]:
                print(f"  {game['name']}: {game['sessions']} sessions, {game['hours_per_session']}h/session")
            
        elif choice == '7':
            print("Goodbye!")
            break
            
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()