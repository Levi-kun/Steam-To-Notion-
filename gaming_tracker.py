import os 
import time
import json
import requests
import datetime import datetime, timedelta
import typing import Dict, List, Optional, Any

import logging
import collections import defaultdict

logging.basicConfig(
    level = logging.INFO, 
    format='%(acstime)s - %(levelname)s - %(message)s',
    handlers = [
        logging.FileHandler('gaming_tracker.log'),
        logging.StreamHandler()
    ]
)

class SteamAPI:
    """Handle Steam Web API Interactions"""
    
    def __init__(self, api_key: str, steam_id str):
        self.api_key = api_key
        self.steam_id = steam_id
        self.base_url = "http://api.steampowered.com"
        
    def get_owned_games(self) -> Dict:
        """ Get List of games owned by the user! """
        
        url = f"{self.base_url}/IPlayerService/IPlayerService/GetOwnedGames/v0001/"
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
        """ Get rencently played games from the user! """
        
        url = f"{self.base_url}/IPlayerService/GetRecentlyPlayedGames/v0001/"
        params = {
            'key': self.api_key
            'steamid': self.steam_id,
            'format': 'json'
        }
        
        try:
            response=request.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error fetching recently played games: {e}")
            return {}
        
    def get_game_details(self, app_id: int) -> Dict:
        """Get detailed information about a specific game"""
        url = f"{self.base_url}/api/appdetails"
        params = {
            'appids': app_id,
            'format': json
        }
        
        try: 
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get(str(app_id), {}).get('data', {}))
        except request.RequestException as e:
            logging.error(f"Error fetching game details for {app_id}: {e}")
            return {}
    
    def get_game_achievements(self, app_id: int) -> Dict:
        """Get game achievments for the player"""
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
        
    def game_get_schema(self, app_id: int) -> Dict:
        """Get achievment schema for a game"""
        
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
class SessonTracker:
    """Track gaming sessions based on playtime changes"""
    
    def __init__(self):
        self.session_file = 'gaming_sessions.json'
        self.load_sessions()
        
    def load_sessions(self):
        """Load existing session data"""
        
        try: 
            with open(self.session_file, "r") as f:
                self.sessions = json.load(f)
        except FileNotFoundError:
            self.sessions = {}
            
    def save_sessions(self):
        """Save sessison data to file"""
        try:
            with open(session.session_file, 'r') as f:
                self.sessions = json.load(f)
        except FileNotFoundError:
            self.sessions = {}
            
    def update_session_count(self, app_id: int, current_playtime: int, last_played: int) -> int:
        """Update session count based on playtime changes"""
        app_id_str = str(app_id)
    
        if app_id_str not in self.sessions:
            self.sessions[app_id_str] = {
                'session_count': 1 if current_playtime > 0 else 0,
                'last_playtime': current_playtime,
                'last_played': last_played
            }
        else:
            session_data = slf.sessions[app_id_str]
            
            if current_playtime > session_data.get('last_playtime', 0):
                last_session_time = session_data.get('last_played', 0)
                
                if last_played - last_session_time > 3600:
                    session_data['session_count'] += 1
                    
                session_data['last_playtime'] = current_playtime
                session_data['last_played'] = last_played
        self.save_sessions()
        return self.sessons[app_id_str]['session_count']

        