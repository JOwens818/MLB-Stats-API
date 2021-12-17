import pandas as pd
import numpy as np
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.metrics import explained_variance_score, r2_score, mean_squared_error
import xgboost
import pickle
import os
import json


class MLBStatPredictor:


    def __init__(self, target):
        self.target = target
        self.dir_path = os.path.abspath(os.curdir)


    def create_and_predict_lin_reg(self, df, deg, year):

        x = df.iloc[:, 3:4].values      # year
        y = df.iloc[:, 28:29].values    # last col is target feature

        poly_reg = PolynomialFeatures(degree=deg)
        x_poly = poly_reg.fit_transform(x)
        poly_reg.fit(x_poly, y)
        lin_reg1 = LinearRegression()
        lin_reg1.fit(x_poly,y)

        predicted = lin_reg1.predict(poly_reg.fit_transform([[year]]))
        score = lin_reg1.score(x_poly,y)

        return {
            "predicted": predicted[0][0],
            "score": score
        }
    


    def create_xgboost_model(self, df, model_type, hyper_params):

        features = df.iloc[:,4:28].columns.tolist()
        target = self.target

        x = df[features]
        y = df[target].values

        X_train, X_test, y_train, y_test = train_test_split(x, y ,test_size=0.2)
        xgb = xgboost.XGBRegressor(**hyper_params)
        xgb.fit(X_train,y_train)

        # Training scores
        training_score = xgb.score(X_train, y_train)
        mean_cv_score = cross_val_score(xgb, X_train, y_train,cv=10).mean()
        kf_cv_scores = cross_val_score(xgb, X_train, y_train, cv=KFold(n_splits=10, shuffle=True)).mean()

        # Testing scores
        predictions = xgb.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        r2s = r2_score(y_test, predictions)
        explained_var_score = explained_variance_score(predictions,y_test)

        # Save model
        # commented out for now to avoid user contention
        # and can possibly be an enhancement in a future iteration
        #self.xgboost_save_model(xgb, model_type)

        return {
            "training": training_score,
            "mean_cv": mean_cv_score,
            "kf_cv": kf_cv_scores,
            "mse": mse,
            "r2s": r2s,
            "explained_var": explained_var_score
        }



    def xgboost_save_model(self, xgb, fileNm):
        file_path = self.dir_path + '/' + fileNm + '.pkl'
        pickle.dump(xgb, open(file_path, "wb"))



    def xgboost_predict(self, stats, fileNm, age=None):
        file_path = self.dir_path + '/' + fileNm + '.pkl'
        xgb_model = pickle.load(open(file_path, "rb"))
        pred = np.array([self.format_player_stats(stats, age)])
        predicted = xgb_model.predict(pred)
        return str(predicted[0])



    def format_player_stats(self, stats, age=None):
        formatted_stats = []

        if age is None:
            formatted_stats.append(stats["player_age"])
        else:
            formatted_stats.append(age)
        
        formatted_stats.append(stats["b_ab"])
        formatted_stats.append(stats["b_total_pa"])
        formatted_stats.append(stats["b_total_hits"])
        formatted_stats.append(stats["b_single"])
        formatted_stats.append(stats["b_double"])
        formatted_stats.append(stats["b_triple"])
        formatted_stats.append(stats["b_home_run"])
        formatted_stats.append(stats["b_strikeout"])
        formatted_stats.append(stats["b_walk"])
        formatted_stats.append(stats["b_k_percent"])
        formatted_stats.append(stats["b_bb_percent"])
        formatted_stats.append(stats["batting_avg"])
        formatted_stats.append(stats["slg_percent"])
        formatted_stats.append(stats["on_base_percent"])
        formatted_stats.append(stats["on_base_plus_slg"])
        formatted_stats.append(stats["isolated_power"])
        formatted_stats.append(stats["b_rbi"])
        formatted_stats.append(stats["b_total_bases"])
        formatted_stats.append(stats["b_ab_scoring"])
        formatted_stats.append(stats["b_game"])
        formatted_stats.append(stats["b_hit_line_drive"])
        formatted_stats.append(stats["b_hit_popup"])
        formatted_stats.append(stats["b_played_dh"])

        return formatted_stats



    def generate_predictions(self, df, year, model_type, xgb_only):
        df_unique = df[['last_name', 'first_name', 'player_id']].drop_duplicates()
        df_final_predictions = pd.DataFrame(columns=['PLAYER_ID', 'MLB_YEAR', 'XWOBA_PREDICTED', 'RSQUARED' ,'MODEL'])
        
        for index, row in df_unique.iterrows():
            df_player = df[(df['player_id'] == row['player_id'])]
            max_year = df_player['mlb_year'].max()
            if year - max_year > 2:
                continue
            
            if len(df_player.index) < 3 or xgb_only:
                prediction = self.predict_player_xgboost(df_player, model_type)
                df_final_predictions.loc[len(df_final_predictions)] = [row['player_id'], year, prediction, None, model_type]
            else:
                prediction = self.predict_player_sklearn(df_player, year)
                df_final_predictions.loc[len(df_final_predictions)] = [row['player_id'], year, prediction['predicted'], prediction['score'], 'sklearn']

        return df_final_predictions
    


    def predict_player_sklearn(self, df, year):
        avg_xwoba = df['xwoba'].mean()
        pred_difference = 99.0
        prediction = {}
        
        for x in range(1,4):
            result = self.create_and_predict_lin_reg(df, x, year)
            calc_diff = abs(result["predicted"] - avg_xwoba)
            if calc_diff < pred_difference:
                pred_difference = calc_diff
                prediction = result

        return prediction



    def predict_player_xgboost(self, df, model_type):
        age = df['player_age'].max() + 1
        dict_mean_vals = df.mean().to_dict()
        return self.xgboost_predict(dict_mean_vals, model_type, age)

