import asyncio
import aiohttp
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import time
import logging

@dataclass
class GameData:
    """Structured game data container"""
    basic_info: Dict
    details: Dict
    achievements: Dict
    session_count: int = 0
    achievement_completion: float = 0

class BatchGameProcessor:
    """Optimized batch processing for Steam-Notion sync"""
    
    def __init__(self, steam_api_key: str, steam_id: str, notion_token: str, notion_database_id: str):
        self.steam_api_key = steam_api_key
        self.steam_id = steam_id
        self.notion_token = notion_token
        self.notion_database_id = notion_database_id
        
        # Batch configuration
        self.STEAM_BATCH_SIZE = 20  # Steam can handle ~20 concurrent requests
        self.NOTION_BATCH_SIZE = 10  # Notion has stricter rate limits
        self.MAX_RETRIES = 3
        
        # Session management
        self.steam_session = None
        self.notion_session = None
    
    async def create_sessions(self):
        """Initialize async HTTP sessions with proper headers"""
        # Steam session
        steam_connector = aiohttp.TCPConnector(limit=25, limit_per_host=25)
        self.steam_session = aiohttp.ClientSession(
            connector=steam_connector,
            timeout=aiohttp.ClientTimeout(total=30)
        )
        
        # Notion session with auth headers
        notion_headers = {
            "Authorization": f"Bearer {self.notion_token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        notion_connector = aiohttp.TCPConnector(limit=15, limit_per_host=15)
        self.notion_session = aiohttp.ClientSession(
            headers=notion_headers,
            connector=notion_connector,
            timeout=aiohttp.ClientTimeout(total=30)
        )
    
    async def close_sessions(self):
        """Properly close async sessions"""
        if self.steam_session:
            await self.steam_session.close()
        if self.notion_session:
            await self.notion_session.close()
    
    async def fetch_game_details_batch(self, app_ids: List[int]) -> Dict[int, Dict]:
        """Fetch game details for multiple games concurrently"""
        async def fetch_single_game(app_id: int) -> tuple[int, Dict]:
            url = "http://store.steampowered.com/api/appdetails"
            params = {'appids': app_id, 'format': 'json'}
            
            for attempt in range(self.MAX_RETRIES):
                try:
                    async with self.steam_session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            return app_id, data.get(str(app_id), {}).get('data', {})
                        elif response.status == 429:  # Rate limited
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            logging.warning(f"Steam API returned {response.status} for app {app_id}")
                except Exception as e:
                    logging.error(f"Error fetching details for {app_id}: {e}")
                    if attempt < self.MAX_RETRIES - 1:
                        await asyncio.sleep(1)
            
            return app_id, {}
        
        # Execute batch requests
        tasks = [fetch_single_game(app_id) for app_id in app_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        game_details = {}
        for result in results:
            if isinstance(result, tuple):
                app_id, details = result
                game_details[app_id] = details
            else:
                logging.error(f"Batch request failed: {result}")
        
        return game_details
    
    async def fetch_achievements_batch(self, app_ids: List[int]) -> Dict[int, float]:
        """Fetch achievement completion percentages for multiple games"""
        async def fetch_single_achievement(app_id: int) -> tuple[int, float]:
            url = f"http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/"
            params = {
                'key': self.steam_api_key,
                'steamid': self.steam_id,
                'appid': app_id,
                'format': 'json'
            }
            
            try:
                async with self.steam_session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        achievements = data.get('playerstats', {}).get('achievements', [])
                        if achievements:
                            completed = sum(1 for ach in achievements if ach.get('achieved') == 1)
                            return app_id, round((completed / len(achievements)) * 100, 1)
                    elif response.status == 403:
                        # Private stats or no achievements
                        pass
            except Exception as e:
                logging.debug(f"Achievement fetch failed for {app_id}: {e}")
            
            return app_id, 0
        
        tasks = [fetch_single_achievement(app_id) for app_id in app_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        achievements = {}
        for result in results:
            if isinstance(result, tuple):
                app_id, completion = result
                achievements[app_id] = completion
        
        return achievements
    
    def process_game_batches(self, games: List[Dict]) -> List[List[int]]:
        """Split games into optimal batches based on API constraints"""
        # Filter valid games first to avoid unnecessary API calls
        valid_app_ids = []
        for game in games:
            # Basic validation - skip obvious non-games
            if game.get('playtime_forever', 0) > 0 or 'name' in game:
                valid_app_ids.append(game.get('appid'))
        
        # Create batches
        batches = []
        for i in range(0, len(valid_app_ids), self.STEAM_BATCH_SIZE):
            batch = valid_app_ids[i:i + self.STEAM_BATCH_SIZE]
            batches.append(batch)
        
        return batches
    
    async def batch_sync_games_to_notion(self, games: List[Dict], include_achievements: bool = True) -> Dict:
        """Optimized batch sync with concurrent API calls and batch Notion updates"""
        logging.info(f"Starting optimized batch sync for {len(games)} games...")
        start_time = time.time()
        
        # Initialize sessions
        await self.create_sessions()
        
        try:
            # Phase 1: Batch fetch all Steam data
            logging.info("Phase 1: Fetching Steam data in batches...")
            app_id_to_game = {game.get('appid'): game for game in games}
            batches = self.process_game_batches(games)
            
            all_game_details = {}
            all_achievements = {} if include_achievements else None
            
            # Process each batch
            for i, batch in enumerate(batches):
                logging.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} games)")
                
                # Concurrent fetch for this batch
                batch_tasks = [
                    self.fetch_game_details_batch(batch)
                ]
                
                if include_achievements:
                    batch_tasks.append(self.fetch_achievements_batch(batch))
                
                batch_results = await asyncio.gather(*batch_tasks)
                
                # Merge results
                all_game_details.update(batch_results[0])
                if include_achievements and len(batch_results) > 1:
                    all_achievements.update(batch_results[1])
                
                # Rate limiting between batches
                if i < len(batches) - 1:  # Don't sleep after last batch
                    await asyncio.sleep(1)
            
            # Phase 2: Process and validate game data
            logging.info("Phase 2: Processing and validating game data...")
            processed_games = []
            
            for app_id, game_details in all_game_details.items():
                if not self.is_valid_game(game_details):
                    continue
                
                basic_game = app_id_to_game.get(app_id, {})
                achievement_completion = all_achievements.get(app_id, 0) if all_achievements else 0
                
                game_data = GameData(
                    basic_info=basic_game,
                    details=game_details,
                    achievements={},
                    achievement_completion=achievement_completion
                )
                processed_games.append(game_data)
            
            # Phase 3: Batch update Notion
            logging.info("Phase 3: Batch updating Notion database...")
            notion_results = await self.batch_update_notion(processed_games)
            
            # Calculate performance metrics
            total_time = time.time() - start_time
            logging.info(f"Optimized sync completed in {total_time:.2f}s")
            logging.info(f"Performance: {len(games)/total_time:.2f} games/second")
            
            return {
                'total_processed': len(processed_games),
                'notion_results': notion_results,
                'processing_time': total_time,
                'performance_games_per_sec': round(len(games)/total_time, 2)
            }
        
        finally:
            await self.close_sessions()
    
    def is_valid_game(self, game_details: Dict) -> bool:
        """Enhanced game validation with caching"""
        # Quick validation checks
        if not game_details or game_details.get('type', '').lower() != 'game':
            return False
        
        genres = game_details.get('genres', [])
        if not genres:
            return False
        
        # Check for non-game categories
        non_game_genres = {'Utilities', 'Software', 'Video Production', 'Animation & Modeling'}
        genre_names = {genre.get('description') for genre in genres}
        
        return not genre_names.issubset(non_game_genres)
    
    async def batch_update_notion(self, game_data_list: List[GameData]) -> Dict:
        """Batch update Notion database with concurrent requests"""
        # First, get existing games to determine create vs update
        existing_games = await self.get_existing_games_async()
        
        create_batches = []
        update_batches = []
        
        # Separate into create/update operations
        for game_data in game_data_list:
            app_id = game_data.basic_info.get('appid')
            
            if app_id in existing_games:
                update_batches.append((existing_games[app_id], game_data))
            else:
                create_batches.append(game_data)
        
        # Process in batches
        created = updated = errors = 0
        
        # Process creates
        for i in range(0, len(create_batches), self.NOTION_BATCH_SIZE):
            batch = create_batches[i:i + self.NOTION_BATCH_SIZE]
            batch_results = await self.create_notion_entries_batch(batch)
            created += batch_results['success']
            errors += batch_results['errors']
            
            # Rate limiting
            await asyncio.sleep(0.5)
        
        # Process updates
        for i in range(0, len(update_batches), self.NOTION_BATCH_SIZE):
            batch = update_batches[i:i + self.NOTION_BATCH_SIZE]
            batch_results = await self.update_notion_entries_batch(batch)
            updated += batch_results['success']
            errors += batch_results['errors']
            
            # Rate limiting
            await asyncio.sleep(0.5)
        
        return {
            'created': created,
            'updated': updated,
            'errors': errors
        }
    
    async def get_existing_games_async(self) -> Dict[int, str]:
        """Asynchronously fetch existing games from Notion"""
        url = f"https://api.notion.com/v1/databases/{self.notion_database_id}/query"
        existing_games = {}
        
        # Implementation would handle pagination and async fetching
        # This is a simplified version
        payload = {"page_size": 100}
        
        try:
            async with self.notion_session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    for page in data.get('results', []):
                        app_id_prop = page.get('properties', {}).get('App ID', {})
                        if app_id_prop.get('number'):
                            existing_games[app_id_prop['number']] = page['id']
        except Exception as e:
            logging.error(f"Error fetching existing games: {e}")
        
        return existing_games
    
    async def create_notion_entries_batch(self, game_data_list: List[GameData]) -> Dict:
        """Create multiple Notion entries concurrently"""
        async def create_single_entry(game_data: GameData) -> bool:
            url = "https://api.notion.com/v1/pages"
            
            # Build properties (similar to original but optimized)
            properties = self.build_notion_properties(game_data)
            payload = {
                "parent": {"database_id": self.notion_database_id},
                "properties": properties
            }
            
            try:
                async with self.notion_session.post(url, json=payload) as response:
                    return response.status == 200
            except Exception as e:
                logging.error(f"Error creating Notion entry: {e}")
                return False
        
        tasks = [create_single_entry(game_data) for game_data in game_data_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success = sum(1 for result in results if result is True)
        errors = len(results) - success
        
        return {'success': success, 'errors': errors}
    
    async def update_notion_entries_batch(self, update_data: List[tuple]) -> Dict:
        """Update multiple Notion entries concurrently"""
        async def update_single_entry(page_id: str, game_data: GameData) -> bool:
            url = f"https://api.notion.com/v1/pages/{page_id}"
            
            # Build update properties
            properties = self.build_notion_update_properties(game_data)
            payload = {"properties": properties}
            
            try:
                async with self.notion_session.patch(url, json=payload) as response:
                    return response.status == 200
            except Exception as e:
                logging.error(f"Error updating Notion entry: {e}")
                return False
        
        tasks = [update_single_entry(page_id, game_data) for page_id, game_data in update_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success = sum(1 for result in results if result is True)
        errors = len(results) - success
        
        return {'success': success, 'errors': errors}
    
    def build_notion_properties(self, game_data: GameData) -> Dict:
        """Build Notion properties from game data"""
        game = game_data.basic_info
        details = game_data.details
        
        # Calculate hours and cost metrics
        playtime_minutes = game.get('playtime_forever', 0)
        hours_played = round(playtime_minutes / 60, 1) if playtime_minutes > 0 else 0
        
        price = details.get('price_overview', {}).get('final', 0) / 100 if details.get('price_overview') else 0
        cost_per_hour = round(price / hours_played, 2) if hours_played > 0 and price > 0 else 0
        
        # Format dates
        last_played_date = None
        if game.get('rtime_last_played'):
            last_played_date = time.strftime('%Y-%m-%d', time.localtime(game.get('rtime_last_played')))
        
        return {
            "Game Name": {"title": [{"text": {"content": details.get('name', 'Unknown')}}]},
            "App ID": {"number": game.get('appid', 0)},
            "Hours Played": {"number": hours_played},
            "Session Count": {"number": game_data.session_count},
            "Achievement Completion": {"number": game_data.achievement_completion},
            "Last Played": {"date": {"start": last_played_date} if last_played_date else None},
            "Genres": {
                "multi_select": [
                    {"name": genre.get('description', '')} 
                    for genre in details.get('genres', [])[:5]
                ]
            },
            "Price": {"number": price},
            "Cost Per Hour": {"number": cost_per_hour},
            "Developer": {
                "rich_text": [
                    {"text": {"content": ', '.join(details.get('developers', []))}}
                ]
            },
            "Status": {"select": {"name": "Owned"}},
            "Platform": {"multi_select": [{"name": "Steam"}]},
        }
    
    def build_notion_update_properties(self, game_data: GameData) -> Dict:
        """Build properties for updating existing entries"""
        game = game_data.basic_info
        playtime_minutes = game.get('playtime_forever', 0)
        hours_played = round(playtime_minutes / 60, 1) if playtime_minutes > 0 else 0
        
        return {
            "Hours Played": {"number": hours_played},
            "Session Count": {"number": game_data.session_count},
            "Achievement Completion": {"number": game_data.achievement_completion}
        }

# Usage example
async def main():
    """Optimized main function using async batch processing"""
    config = {
        'STEAM_API_KEY': 'your_key_here',
        'STEAM_ID': 'your_id_here', 
        'NOTION_TOKEN': 'your_token_here',
        'NOTION_DATABASE_ID': 'your_db_id_here'
    }
    
    processor = BatchGameProcessor(
        config['STEAM_API_KEY'],
        config['STEAM_ID'], 
        config['NOTION_TOKEN'],
        config['NOTION_DATABASE_ID']
    )
    
    # This would integrate with your existing game fetching logic
    # games = fetch_owned_games()  # Your existing method
    
    # Example with dummy data
    games = []  # Replace with actual Steam games list
    
    results = await processor.batch_sync_games_to_notion(games, include_achievements=True)
    print(f"Batch sync results: {results}")

if __name__ == "__main__":
    asyncio.run(main())