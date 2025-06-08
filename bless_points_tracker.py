import json
import logging
import schedule
import time
import random
from datetime import datetime, timedelta
import cloudscraper
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import traceback
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
# MongoDB connection
MONGODB_URI = os.environ.get("MONGODB_URI")

# Bless Network APIs
BLESS_EARNINGS_API_URL = "https://gateway-run-indexer.bls.dev/api/v1/users/earnings"
BLESS_OVERVIEW_API_URL = "https://gateway-run-indexer.bls.dev/api/v1/users/overview"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bless_uptime_tracker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BlessUptimeTracker:
    def __init__(self):
        self.tokens_file = "bless_tokens.json"
        self.db_client = None
        self.db = None
        self.collection = None
        self.scraper = cloudscraper.create_scraper()
        self.proxies = self.load_proxies()
        self.setup_database()

    def setup_database(self):
        """Initialize MongoDB connection"""
        try:
            self.db_client = MongoClient(MONGODB_URI)
            self.db = self.db_client['bless_farming']
            self.collection = self.db['bless_uptime_tracker']  # Use unique collection name
            
            # Create indexes for better performance
            self.collection.create_index([("user_id", 1), ("timestamp", -1)])
            # Separate indexes for different document types
            self.collection.create_index([("type", 1), ("user_id", 1)], unique=True, 
                                       partialFilterExpression={"type": "summary"})
            self.collection.create_index([("type", 1), ("user_id", 1), ("date", 1)], unique=True,
                                       partialFilterExpression={"type": "daily_uptime"})
            
            # Test connection
            self.db_client.admin.command('ping')
            logger.info("MongoDB connection established")
            
        except ConnectionFailure as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise

    def load_tokens(self):
        """Load JWT tokens from file"""
        try:
            with open(self.tokens_file, "r") as f:
                data = json.load(f)
                tokens = data.get("tokens", [])
                logger.info(f"Loaded {len(tokens)} tokens from {self.tokens_file}")
                return tokens
        except FileNotFoundError:
            logger.error(f"{self.tokens_file} not found. Creating template...")
            self.create_tokens_template()
            return []
        except Exception as e:
            logger.error(f"Error loading tokens: {e}")
            return []

    def create_tokens_template(self):
        """Create template tokens.json file"""
        template = {
            "tokens": [
                {
                    "name": "Account 1",
                    "jwt_token": "YOUR_JWT_TOKEN_HERE",
                    "user_id": "YOUR_USER_ID_HERE",
                    "pubkey": "YOUR_PUBKEY_HERE"
                }
            ]
        }
        with open(self.tokens_file, "w") as f:
            json.dump(template, f, indent=2)
        logger.info(f"Created template {self.tokens_file}")

    def load_proxies(self):
        try:
            with open("proxy.txt", "r") as f:
                proxies = [line.strip() for line in f if line.strip()]
                if proxies:
                    logger.info(f"Loaded {len(proxies)} proxies from proxy.txt")
                else:
                    logger.warning("proxy.txt is empty, not using proxies")
                return proxies
        except FileNotFoundError:
            logger.warning("proxy.txt not found, not using proxies")
            return []

    def get_random_proxy(self):
        if self.proxies:
            return random.choice(self.proxies)
        return None

    def fetch_overview_data(self, token, proxy=None):
        headers = {
            "Authorization": f"Bearer {token}"
        }
        try:
            logger.info("Making overview API request...")
            response = self.scraper.get(BLESS_OVERVIEW_API_URL, headers=headers, timeout=30, proxy=proxy)
            logger.info(f"Overview API Response Status: {response.status_code}")
            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info("Successfully fetched overview data")
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    return None
            else:
                logger.error(f"Overview API failed with status: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching overview: {e}")
            return None

    def fetch_uptime_data(self, token, pubkey, proxy=None):
        headers = {
            "Authorization": f"Bearer {token}"
        }
        try:
            logger.info("Making earnings API request...")
            response = self.scraper.get(BLESS_EARNINGS_API_URL, headers=headers, timeout=30, proxy=proxy)
            logger.info(f"Earnings API Response Status: {response.status_code}")
            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"Successfully fetched {len(data) if isinstance(data, list) else 0} uptime records")
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    logger.error(f"Response: {response.text[:500]}")
                    return None
            elif response.status_code == 401:
                logger.error("Authentication failed - token may be expired")
                logger.error(f"Response: {response.text}")
                return None
            elif response.status_code == 403:
                logger.error("Access forbidden - Cloudflare blocking request")
                logger.error(f"Response: {response.text[:500]}")
                return None
            else:
                logger.error(f"Unexpected status: {response.status_code}")
                logger.error(f"Response: {response.text[:500]}")
                return None
        except Exception as e:
            logger.error(f"Error fetching uptime: {e}")
            return None

    def calculate_daily_uptime(self, uptime_records, user_id):
        """Calculate daily uptime from cumulative data"""
        if not uptime_records:
            return []
        
        # Sort by date to ensure proper order
        sorted_records = sorted(uptime_records, key=lambda x: x.get('date', ''))
        daily_uptime = []
        
        prev_base = 0
        prev_total = 0
        prev_referral = 0
        
        for record in sorted_records:
            current_base = record.get('baseReward', 0)
            current_total = record.get('totalReward', 0) 
            current_referral = record.get('referralReward', 0)
            
            # Calculate daily differences
            daily_base = max(0, current_base - prev_base)
            daily_total = max(0, current_total - prev_total)
            daily_ref = max(0, current_referral - prev_referral)
            
            daily_uptime.append({
                'date': record.get('date'),
                'daily_base_minutes': daily_base,
                'daily_total_minutes': daily_total,
                'daily_referral_minutes': daily_ref,
                'daily_base_hours': round(daily_base / 60, 2),
                'daily_total_hours': round(daily_total / 60, 2),
                'cumulative_base_minutes': current_base,
                'cumulative_total_minutes': current_total,
                'cumulative_referral_minutes': current_referral
            })
            
            prev_base = current_base
            prev_total = current_total
            prev_referral = current_referral
        
        return daily_uptime

    def save_to_database(self, uptime_data, overview_data, account_name, user_id, pubkey):
        """Save uptime data to MongoDB using upsert operations"""
        try:
            timestamp = datetime.now()
            
            if not uptime_data and not overview_data:
                logger.warning(f"No data for {account_name}")
                return
                
            # Calculate daily uptime from earnings data if available
            daily_records = []
            if uptime_data:
                daily_records = self.calculate_daily_uptime(uptime_data, user_id)
            
            # Use overview data for current totals (more accurate)
            if overview_data:
                today_base = overview_data.get('todayBaseReward', 0)
                today_total = overview_data.get('todayTotalReward', 0) 
                today_referral = overview_data.get('todayReferralsReward', 0)
                
                alltime_base = overview_data.get('allTimeBaseReward', 0)
                alltime_total = overview_data.get('allTimeTotalReward', 0)
                alltime_referral = overview_data.get('allTimeReferralsReward', 0)
                
                # Save/update user summary using overview data
                summary_doc = {
                    "type": "summary",
                    "user_id": user_id,
                    "account_name": account_name,
                    "pubkey": pubkey,
                    "timestamp": timestamp,
                    "today_base_minutes": today_base,
                    "today_total_minutes": today_total,
                    "today_referral_minutes": today_referral,
                    "alltime_base_minutes": alltime_base,
                    "alltime_total_minutes": alltime_total,
                    "alltime_referral_minutes": alltime_referral,
                    "today_total_hours": round(today_total / 60, 2),
                    "alltime_total_hours": round(alltime_total / 60, 2),
                    "alltime_total_days": round(alltime_total / (60 * 24), 2),
                    "total_days_tracked": len(daily_records) if daily_records else 0
                }
                
                # Calculate detailed time breakdown
                total_minutes = alltime_total
                total_hours = total_minutes / 60
                total_days = total_hours / 24
                days = int(total_days)
                remaining_hours = total_hours - (days * 24)
                hours = int(remaining_hours)
                minutes = int((remaining_hours - hours) * 60)
                
                summary_doc.update({
                    "participation_time_breakdown": {
                        "days": days,
                        "hours": hours, 
                        "minutes": minutes,
                        "total_formatted": f"{days} days, {hours} hours, {minutes} minutes"
                    }
                })
                
                # Upsert summary document
                result = self.collection.replace_one(
                    {"type": "summary", "user_id": user_id},
                    summary_doc,
                    upsert=True
                )
                
                action = "Updated" if result.matched_count > 0 else "Created"
                logger.info(f"{action} overview summary for {account_name}: {alltime_total:,} total minutes ({days}d {hours}h {minutes}m)")

            # Save daily records if available
            if daily_records:
                saved_count = 0
                updated_count = 0
                
                for daily_record in daily_records:
                    daily_doc = {
                        "type": "daily_uptime",
                        "user_id": user_id,
                        "account_name": account_name,
                        "pubkey": pubkey,
                        "timestamp": timestamp,
                        **daily_record
                    }
                    
                    # Upsert daily uptime document
                    result = self.collection.replace_one(
                        {"type": "daily_uptime", "user_id": user_id, "date": daily_record['date']},
                        daily_doc,
                        upsert=True
                    )
                    
                    if result.matched_count > 0:
                        updated_count += 1
                    else:
                        saved_count += 1
                        
                logger.info(f"Daily uptime for {account_name}: {saved_count} new, {updated_count} updated")
                
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            logger.error(traceback.format_exc())

    def process_account(self, account):
        max_retries = 3
        retry_delay = 10
        for attempt in range(max_retries):
            try:
                name = account.get('name', 'Unknown')
                token = account.get('jwt_token')
                user_id = account.get('user_id')
                pubkey = account.get('pubkey')
                if not token or not user_id or not pubkey:
                    logger.warning(f"Skipping {name} - missing token, user_id, or pubkey")
                    return
                logger.info(f"Processing account: {name} (attempt {attempt + 1}/{max_retries})")
                if attempt > 0:
                    time.sleep(retry_delay)
                proxy = self.get_random_proxy()
                logger.info(f"Using proxy {proxy} for account: {name}")
                overview_data = self.fetch_overview_data(token, proxy=proxy)
                uptime_data = self.fetch_uptime_data(token, pubkey, proxy=proxy)
                if overview_data is not None or uptime_data is not None:
                    self.save_to_database(uptime_data, overview_data, name, user_id, pubkey)
                    logger.info(f"Successfully completed processing: {name}")
                    return
                else:
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying {name} in {retry_delay} seconds...")
                    else:
                        logger.error(f"Failed to process {name} after {max_retries} attempts")
            except Exception as e:
                logger.error(f"Error processing account {name}: {e}")

    def run_tracking_cycle(self):
        """Run one complete tracking cycle for all accounts"""
        logger.info("Starting Bless uptime tracking cycle...")
        
        tokens = self.load_tokens()
        if not tokens:
            logger.warning("No tokens loaded. Please check bless_tokens.json")
            return
            
        # Process accounts one by one
        for i, account in enumerate(tokens):
            if i > 0:  # Add delay between accounts
                delay = random.uniform(5, 10)
                logger.info(f"Waiting {delay:.1f} seconds before next account...")
                time.sleep(delay)
            
            self.process_account(account)
        
        logger.info("Bless uptime tracking cycle completed")

    def get_uptime_stats(self):
        """Get simplified uptime stats for dashboard integration"""
        try:
            # Get current uptime for all accounts
            summary_pipeline = [
                {"$match": {"type": "summary"}},
                {"$sort": {"timestamp": -1}},
                {"$group": {
                    "_id": "$user_id",
                    "account_name": {"$first": "$account_name"},
                    "today_total_minutes": {"$first": "$today_total_minutes"},
                    "alltime_total_minutes": {"$first": "$alltime_total_minutes"},
                    "alltime_base_minutes": {"$first": "$alltime_base_minutes"},
                    "alltime_referral_minutes": {"$first": "$alltime_referral_minutes"},
                    "participation_time_breakdown": {"$first": "$participation_time_breakdown"},
                    "last_updated": {"$first": "$timestamp"}
                }}
            ]
            
            summary_results = list(self.collection.aggregate(summary_pipeline))
            
            if not summary_results:
                print("📊 Bless Tracker: No data available")
                return
            
            total_today_minutes = 0
            total_alltime_minutes = 0
            
            print(f"\n📊 BLESS UPTIME SUMMARY")
            print("-" * 50)
            
            for result in summary_results:
                today_min = result.get('today_total_minutes', 0)
                alltime_min = result.get('alltime_total_minutes', 0)
                breakdown = result.get('participation_time_breakdown', {})
                
                total_today_minutes += today_min
                total_alltime_minutes += alltime_min
                
                # Format today's time
                today_hours = today_min // 60
                today_mins = today_min % 60
                today_formatted = f"{today_hours}h {today_mins}m"
                
                print(f"🔹 {result['account_name']}:")
                print(f"   Today: {today_formatted}")
                if breakdown:
                    print(f"   Total: {breakdown.get('total_formatted', 'N/A')}")
                print()
            
            # Overall summary
            print(f"📅 Total Today: {total_today_minutes//60}h {total_today_minutes%60}m")
            print(f"🏆 Total All-time: {total_alltime_minutes//60:.0f}h ({total_alltime_minutes/(60*24):.1f} days)")
            print(f"👥 Accounts: {len(summary_results)}")
            print("-" * 50)
            
        except Exception as e:
            logger.error(f"Error getting uptime stats: {e}")

def run_sync_job():
    """Wrapper to run sync function in scheduler"""
    tracker = BlessUptimeTracker()
    tracker.run_tracking_cycle()

def main():
    logger.info("Bless Uptime Tracker Started")
    
    tracker = BlessUptimeTracker()
    
    # Schedule tracking every hour
    schedule.every().hour.do(run_sync_job)
    
    # Run once immediately
    logger.info("Running initial uptime tracking cycle...")
    tracker.run_tracking_cycle()
    tracker.get_uptime_stats()
    
    # Keep running
    logger.info("Scheduled to run every hour. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        if tracker.db_client:
            tracker.db_client.close()

if __name__ == "__main__":
    main() 