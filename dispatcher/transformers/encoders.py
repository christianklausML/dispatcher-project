import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
import math
import numpy as np

class TimeFeaturesEncoder(BaseEstimator, TransformerMixin):
    """
        Extract the  the hour in a sine and cosine manner from time columns.
        Returns a copy of the DataFrame X with 'hr_cols' columns appended.
    """
    def __init__(self, hr_cols=None):
        if hr_cols is None:
            self.hr_cols = ['PLANNED_PICKUP_TIMESTAMP', 'CREATED_AT_SHIPMENT']
        else:
            self.hr_cols = hr_cols
        
        
    def fit(self, X, y=None):
        return self
            
    def transform(self, X, y=None):
        X_ = X.copy()
        #X_ = pd.DataFrame()
        for hr_col in self.hr_cols:
            X_[f'{hr_col}_HR'] = X[hr_col].dt.hour
            X_[f'{hr_col}_HR_norm'] = 2 * math.pi * X_[f'{hr_col}_HR'] / X_[f'{hr_col}_HR'].max()
            X_[f'{hr_col}_HR_cos'] = np.cos(X_[f'{hr_col}_HR_norm'])
            X_[f'{hr_col}_HR_sin'] = np.sin(X_[f'{hr_col}_HR_norm'])
            X_ = X_.drop(columns=[f'{hr_col}_HR_norm', f'{hr_col}_HR'])
            
        return X_
    
class DayFeaturesEncoder(BaseEstimator, TransformerMixin):
    """
        Extract the  the dow in a sine and cosine manner from time columns.
        Returns a copy of the DataFrame X with 'dow_cols' columns appended.
    """
    def __init__(self, dow_cols=None):
        if dow_cols is None:
            self.dow_cols = ['PLANNED_PICKUP_TIMESTAMP', 'CREATED_AT_SHIPMENT']
        else:
            self.dow_cols = dow_cols
        
    def fit(self, X, y=None):
        return self
            
    def transform(self, X, y=None):
        X_ = X.copy()
        #X_ = pd.DataFrame()
        for dow_col in self.dow_cols:
            X_[f'{dow_col}_DOW'] = X[dow_col].dt.dayofweek
            X_[f'{dow_col}_DOW_norm'] = 2 * math.pi * X_[f'{dow_col}_DOW'] / X_[f'{dow_col}_DOW'].max()
            X_[f'{dow_col}_DOW_cos'] = np.cos(X_[f'{dow_col}_DOW_norm'])
            X_[f'{dow_col}_DOW_sin'] = np.sin(X_[f'{dow_col}_DOW_norm'])
            X_ = X_.drop(columns=[f'{dow_col}_DOW_norm', f'{dow_col}_DOW'])
        
        X_ = X_.drop(columns=self.dow_cols)

        return X_
      
