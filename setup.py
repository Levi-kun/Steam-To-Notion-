"""

Setup script for Steam-Notion Gaming Tracker (index.py)

"""

import os
import requests

from datetime import datetime

def test_steam_api(api_key, steam_id):
    print("Testing Steam API Connection")
    
    url = "http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/"
    params = {
        'key': api_key,
        'steamids': steam_id,
        'format': 'json'
    }
    
    try: 
        response = request.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'response' in data and 'players' in data['response'] and data['response']['players']:
            player = data['response']['players'][0]
             print(f"✅ Steam API working! Connected to: {player.get('personaname', 'Unknown')}")
            return True
        else:
            print("❌ Steam API returned empty response. Check your Steam ID.")
            return False
    except requests.RequestException as e:
        print(f"❌ Steam API connection failed: {e}")
        return False    
    
def test_notion_api(token, database_id):
    """Test Notion API connection"""
    print("Testing Notion API connection...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    url = f"https://api.notion.com/v1/databases/{database_id}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        print(f"✅ Notion API working! Connected to database: {data.get('title', [{}])[0].get('text', {}).get('content', 'Unnamed Database')}")
        
        # Check if database has required properties
        properties = data.get('properties', {})
        required_props = [
            'Game Name', 'App ID', 'Hours Played', 'Session Count',
            'Gameplay Rating', 'Overall Rating'
        ]
        
        missing_props = [prop for prop in required_props if prop not in properties]
        
        if missing_props:
            print(f"⚠️  Missing database properties: {', '.join(missing_props)}")
            print("Please add these properties to your Notion database.")
            return False
        else:
            print("✅ All required database properties found!")
            return True
            
    except requests.RequestException as e:
        print(f"❌ Notion API connection failed: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")
        return False
def create_env_file(config):
    """ Create .env file with configuration """
    
    env.content = f"""# Steam-Notion Gaming Tracker Configuration # Generated on {datetime.now().strftime('%Y-%m-d %H:%M:%S')}
    STEAM_API_KEY={config['STEAM_API_KEY']}
    STEAM_ID={config['STEAM_ID']}
    NOTION_TOKEN={config['NOTION_TOKEN']}
    NOTION_DATABASE_ID={config['NOTION_DATABASE_ID']}"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print("✅ Created Enviorment File! ✅")
    
def main():
    print("Steam-Notion Gaming Tracker Setup")
    print("=" * 39)

    config = {}
    
    print("\n1. Steam API Configuration")
    print("Get your API key from: https://steamcommunity.com/dev/apikey")
    config['STEAM_API_KEY'] = input("Enter your Steam API Key: ").strip()
    
    print("\nFind your Steam ID at: https://steamid.io/")
    config['STEAM_ID'] = input("Enter your Steam ID (long number): ").strip()
    
    print("\n2. Notion API Configuration")
    print("Create integration at: https://developers.notion.com/")
    config['NOTION_TOKEN'] = input("Enter your Notion Integration Token: ").strip()
    
    print("\nShare your database with the integration, then copy the database ID from URL")
    config['NOTION_DATABASE_ID'] = input("Enter your Notion Database ID: ").strip()
    
    # Validate configuration
    print("\n3. Validating Configuration")
    print("-" * 30)
    
    steam_ok = test_steam_api(config['STEAM_API_KEY'], config['STEAM_ID'])
    notion_ok = test_notion_api(config['NOTION_TOKEN'], config['NOTION_DATABASE_ID'])
    
    if steam_ok and notion_ok:
        print("\n🎉 All APIs working correctly!")
        
        # Ask if user wants to save config
        save_config = input("\nSave configuration to .env file? (y/n): ").lower().startswith('y')
        
        if save_config:
            create_env_file(config)
            
        print("\n📋 Next Steps:")
        print("1. Run 'python gaming_tracker.py' to start using the tracker")
        print("2. For GitHub Actions automation, add these values as repository secrets")
        print("3. The first sync may take a while as it processes your entire library")
        
    else:
        print("\n❌ Setup incomplete. Please check your API keys and database configuration.")
        print("\n🔧 Troubleshooting:")
        if not steam_ok:
            print("- Verify your Steam API key is correct")
            print("- Ensure your Steam profile is public")
            print("- Check that the Steam ID is the 64-bit Steam ID (not custom URL)")
        
        if not notion_ok:
            print("- Verify your Notion integration token")
            print("- Ensure the database is shared with your integration")
            print("- Create all required database properties (see documentation)")

if __name__ == "__main__":
    main()