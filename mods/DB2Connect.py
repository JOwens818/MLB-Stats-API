import ibm_db
import ibm_db_dbi
import ibm_db_sa
import sqlalchemy as sa
import pandas as pd


class DB2Connect:


    def __init__(self, db2_creds):
        db2_conn_str = f"db2+ibm_db://{db2_creds['user']}:{db2_creds['pw']}@{db2_creds['host']}:{db2_creds['port']}/{db2_creds['db']}"
        try:
            self.engine = sa.create_engine(db2_conn_str)
            self.conn = self.engine.connect()
        except sa.exc.SQLAlchemyError as sa_err:
            raise sa_err



    def get_all_data(self, start_yr, end_yr):
        params = [start_yr, end_yr]
        sql = "SELECT * FROM mlbstats WHERE mlb_year >= ? AND mlb_year <= ?"
        return pd.read_sql(sql, con=self.conn, params=params)



    def save_xgb_scores(self, scores, model_type):

        # Delete current scores
        params = [model_type]
        sql = "DELETE FROM XGBOOST_SCORES WHERE MODEL_TYPE = ?"
        self.engine.execute(sql, con=self.conn, *params)

        #Insert new record with new score
        params = [
            model_type,
            scores['training'],
            scores['mean_cv'],
            scores['kf_cv'],
            scores['mse'],
            scores['r2s'],
            scores['explained_var']
        ]

        sql = (
            "INSERT INTO XGBOOST_SCORES(MODEL_TYPE, TRAINING, MEAN_CV, KFOLD_CV_AVG, MSE, RSQUARED, EXPLAINED_VAR) "
            "VALUES (?,?,?,?,?,?,?)"
        )
        self.engine.execute(sql, con=self.conn, *params)



    def save_xgb_hyperparams(self, hyper_params, model_type):

        # Delete current scores
        params = [model_type]
        sql = "DELETE FROM XGBOOST_HYPERPARAMS WHERE MODEL_TYPE = ?"
        self.engine.execute(sql, con=self.conn, *params)

        #Insert new record with new score
        params = [
            model_type,
            hyper_params['n_estimators'],
            hyper_params['subsample'],
            hyper_params['max_depth'],
            hyper_params['learning_rate'],
            hyper_params['gamma'],
            hyper_params['reg_alpha'],
            hyper_params['reg_lambda']
        ]

        sql = (
            "INSERT INTO XGBOOST_HYPERPARAMS(MODEL_TYPE, N_ESTIMATORS, SUBSAMPLE, MAX_DEPTH, LEARNING_RATE, GAMMA, REG_ALPHA, REG_LAMBDA) "
            "VALUES (?,?,?,?,?,?,?,?)"
        )
        self.engine.execute(sql, con=self.conn, *params)