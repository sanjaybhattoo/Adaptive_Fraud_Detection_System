import logging
from typing import Dict, Any
from datetime import datetime
import math

logger = logging.getLogger(__name__)
class FeatureComputer:
    def __init__(self, cache, db):
        self.cache = cache
        self.db = db
    
    def compute_velocity_features(self, user_id: str, txn: Dict[str, Any]) -> Dict[str, Any]:
        try:
            velocity_1h_key = f"velocity:1h:{user_id}"
            velocity_1h = self.cache.get_int(velocity_1h_key) or 0
            self.cache.increment(velocity_1h_key, expire=3600)
            velocity_24h_key = f"velocity:24h:{user_id}"
            velocity_24h = self.cache.get_int(velocity_24h_key) or 0
            self.cache.increment(velocity_24h_key, expire=86400)
            merchants_24h_key = f"merchants:24h:{user_id}"
            merchant_id = txn.get('merchant_id')
            self.cache.add_to_set(merchants_24h_key, merchant_id, expire=86400)
            unique_merchants = self.cache.get_set_size(merchants_24h_key)
            cards_24h_key = f"cards:24h:{user_id}"
            card_id = txn.get('card_id')
            self.cache.add_to_set(cards_24h_key, card_id, expire=86400)
            unique_cards = self.cache.get_set_size(cards_24h_key)
            ips_24h_key = f"ips:24h:{user_id}"
            ip_address = txn.get('ip_address')
            self.cache.add_to_set(ips_24h_key, ip_address, expire=86400)
            unique_ips = self.cache.get_set_size(ips_24h_key)
            
            return {
                'velocity_1h': velocity_1h,
                'velocity_24h': velocity_24h,
                'unique_merchants_24h': unique_merchants,
                'unique_cards_24h': unique_cards,
                'unique_ips_24h': unique_ips,
            }
        
        except Exception as e:
            logger.error(f"Failed to compute velocity features: {e}")
            return {
                'velocity_1h': 0,
                'velocity_24h': 0,
                'unique_merchants_24h': 0,
                'unique_cards_24h': 0,
                'unique_ips_24h': 0,
            }
    
    def compute_behavioral_features(self, user_id: str, txn: Dict[str, Any]) -> Dict[str, Any]:
        try:
            timestamp = txn.get('timestamp', '')
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                txn_hour = dt.hour
            except:
                txn_hour = -1
            device_key = f"device:{user_id}"
            current_device = txn.get('device_id')
            previous_device = self.cache.get(device_key)
            device_changed = current_device != previous_device if previous_device else False
            
            if current_device:
                self.cache.set(device_key, current_device, expire=86400)
            
            # Billing != Shipping address
            address_mismatch = (
                txn.get('billing_address') != 
                txn.get('shipping_address')
            )
            
            return {
                'transaction_hour': txn_hour,
                'device_changed': int(device_changed),
                'address_mismatch': int(address_mismatch),
            }
        
        except Exception as e:
            logger.error(f"Failed to compute behavioral features: {e}")
            return {
                'transaction_hour': -1,
                'device_changed': 0,
                'address_mismatch': 0,
            }
    
    def compute_historical_features(self, user_id: str) -> Dict[str, Any]:
        try:
            stats = self.db.get_user_stats(user_id, days=30)
            
            if stats:
                return {
                    'avg_amount_30d': stats.get('avg_amount', 0.0),
                    'txn_count_30d': stats.get('txn_count', 0),
                    'max_amount_30d': stats.get('max_amount', 0.0),
                    'min_amount_30d': stats.get('min_amount', 0.0),
                }
            else:
                return {
                    'avg_amount_30d': 0.0,
                    'txn_count_30d': 0,
                    'max_amount_30d': 0.0,
                    'min_amount_30d': 0.0,
                }
        
        except Exception as e:
            logger.error(f"historical features: {e}")
            return {
                'avg_amount_30d': 0.0,
                'txn_count_30d': 0,
                'max_amount_30d': 0.0,
                'min_amount_30d': 0.0,
            }
    
    def compute_geographic_features(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        try:
            user_location = txn.get('user_location', {})
            if isinstance(user_location, dict):
                lat = user_location.get('lat')
                lng = user_location.get('lng')
            else:
                lat, lng = None, None
            
            return {
                'current_latitude': float(lat) if lat else 0.0,
                'current_longitude': float(lng) if lng else 0.0,
            }
        
        except Exception as e:
            logger.error(f"geographic features: {e}")
            return {
                'current_latitude': 0.0,
                'current_longitude': 0.0,
            }
