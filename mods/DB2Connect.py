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



    def get_histogram_data(self, fieldNm):
        
        # Ensure valid field name is requested
        sql = "SELECT NAME AS field FROM SYSIBM.SYSCOLUMNS WHERE TBcreator = 'MLN78422' and TBNAME = 'MLBSTATS'"
        fields = pd.read_sql(sql, con=self.conn)
        if not fieldNm in fields['field'].values:
            return False

        # Now get data
        sql = f'SELECT \'histdata\' as "group", {fieldNm} as "value" from mlbstats'
        return pd.read_sql(sql, con=self.conn).to_json(orient='records')
        


    def get_histogram_stats(self, fieldNm):

        # Ensure valid field name is requested
        sql = "SELECT NAME AS field FROM SYSIBM.SYSCOLUMNS WHERE TBcreator = 'MLN78422' and TBNAME = 'MLBSTATS'"
        fields = pd.read_sql(sql, con=self.conn)
        if not fieldNm in fields['field'].values:
            return False
        
        # Now get data
        sql = (
            f'SELECT MAX({fieldNm}) as "max", '
            f'MIN({fieldNm}) as "min", '
            f'AVG({fieldNm}) as "avg", '
            f'MEDIAN({fieldNm}) as "median", '
            
            f'(SELECT {fieldNm} '
            'FROM MLBSTATS '
            f'GROUP BY {fieldNm} '
            f'ORDER BY COUNT({fieldNm}) DESC '
            'LIMIT 1) as "mode" '

            'from mlbstats'
        )
        return pd.read_sql(sql, con=self.conn).to_json(orient='records')



    def get_scatter_data(self, fieldNm):

        # Ensure valid field name is requested
        sql = "SELECT NAME AS field FROM SYSIBM.SYSCOLUMNS WHERE TBcreator = 'MLN78422' and TBNAME = 'MLBSTATS'"
        fields = pd.read_sql(sql, con=self.conn)
        if not fieldNm in fields['field'].values:
            return False

        sql = f'SELECT \'scatterdata\' as "group", XWOBA as "xwoba", {fieldNm} as "selectedField" FROM MLBSTATS'
        return pd.read_sql(sql, con=self.conn).to_json(orient='records')



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



    def delete_model_predictions_by_year(self, year):
        params = [year]
        sql = "DELETE FROM PLAYER_PREDICTIONS WHERE MLB_YEAR = ?"
        self.engine.execute(sql, con=self.conn, *params)



    def append_to_table(self, df, tblNm, if_exists):
        df.to_sql(
            tblNm,
            self.engine,
            if_exists=if_exists,
            index=False,
            chunksize=500,
            method="multi",
            dtype=self.get_table_datatypes(tblNm)
        )



    def get_table_datatypes(self, tblNm):

        if tblNm == "player_predictions":
            return {
                "PLAYER_ID": sa.types.INTEGER,
                "MLB_YEAR": sa.types.SMALLINT,
                "XWOBA_PREDICTED": sa.types.DECIMAL,
                "RSQUARED": sa.types.REAL,
                "MODEL": sa.types.VARCHAR(50)
            }