import logging
import os
from typing import Tuple, Optional

import joblib
import xgboost as xgb
import numpy as np

logger = logging.getLogger(__name__)
class FraudDetectionModel:
    
    def __init__(self, model_path: str, scaler_path: str):
        self.model_path = model_path
        self.scaler_path = scaler_path
        self.model = self._load_model(model_path)
        self.scaler = self._load_scaler(scaler_path)
        
        logger.info(f"Model loaded from {model_path}")
    
    def _load_model(self, path: str) -> xgb.XGBClassifier:
        if not os.path.exists(path):
            logger.warning(f"Model not found at {path}, using dummy model")
            return self._create_dummy_model()
        
        try:
            model = joblib.load(path)
            logger.info(f"Loaded model from {path}")
            return model
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return self._create_dummy_model()
    
    def _load_scaler(self, path: str):
        if not os.path.exists(path):
            logger.warning(f"Scaler not found at {path}")
            return None
        
        try:
            scaler = joblib.load(path)
            logger.info(f"Loaded scaler from {path}")
            return scaler
        except Exception as e:
            logger.error(f"Failed to load scaler: {e}")
            return None
    
    def _create_dummy_model(self):
        logger.info("Creating dummy model for testing")
        model = xgb.XGBClassifier(
            n_estimators=10,
            max_depth=4,
            learning_rate=0.1,
            random_state=42
        )
        return model
    
    def predict(self, feature_vector: np.ndarray) -> Tuple[float, np.ndarray]:
        try:
            if len(feature_vector.shape) == 1:
                feature_vector = feature_vector.reshape(1, -1)
            if self.scaler:
                feature_vector = self.scaler.transform(feature_vector)
            proba = self.model.predict_proba(feature_vector)[0]
            fraud_probability = float(proba[1]) 
            feature_importances = self.model.feature_importances_
            
            return fraud_probability, feature_importances
        
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return 0.5, np.zeros(30)  # Default: medium risk
    
    def get_feature_importance(self) -> np.ndarray:
        return self.model.feature_importances_
    
    def get_model_info(self) -> dict:
        return {
            'model_type': 'XGBoost',
            'n_estimators': self.model.n_estimators,
            'max_depth': self.model.max_depth,
            'learning_rate': self.model.learning_rate,
            'has_scaler': self.scaler is not None,
        }
