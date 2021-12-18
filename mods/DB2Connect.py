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



    def get_radar_player_data(self, player_id):
        params = [player_id, player_id, player_id, player_id, player_id]

        sql = (
            'SELECT CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME) AS "player", '
            '\'Home Runs\' AS "stat", '
            'SUM(B_HOME_RUN)  AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '
            'GROUP BY CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME), \'Home Runs\' '

            'UNION '
            'SELECT CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME) AS "player", '
            '\'RBI\' AS "stat", '
            'SUM(B_RBI)  AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '
            'GROUP BY CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME), \'RBI\' '

            'UNION '
            'SELECT CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME) AS "player", '
            '\'Hits\' AS "stat", '
            'SUM(B_TOTAL_HITS)  AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '
            'GROUP BY CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME), \'Hits\' '

            'UNION '
            'SELECT CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME) AS "player", '
            '\'Strikeouts\' AS "stat", '
            'SUM(B_STRIKEOUT)  AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '
            'GROUP BY CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME), \'Strikeouts\' '

            'UNION '
            'SELECT CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME) AS "player", '
            '\'Walks\' AS "stat", '
            'SUM(B_WALK) AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '
            'GROUP BY CONCAT(CONCAT(FIRST_NAME, \' \'), LAST_NAME), \'Walks\' '
        )
        return pd.read_sql(sql, con=self.conn, params=params).to_json(orient='records')



    def get_line_player_data(self, player_id):
        params = [player_id, player_id, player_id, player_id, player_id]
        sql = (
            'SELECT \'xwOBA\' AS "group", '
            'MLB_YEAR AS "key", '
            'XWOBA AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '

            'UNION '
            'SELECT \'AVG\' AS "group", '
            'MLB_YEAR AS "key", '
            'BATTING_AVG AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '

            'UNION '
            'SELECT \'OPS\' AS "group", '
            'MLB_YEAR AS "key", '
            'ON_BASE_PLUS_SLG AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '

            'UNION '
            'SELECT \'SLG\' AS "group", '
            'MLB_YEAR AS "key", '
            'SLG_PERCENT AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '

            'UNION '
            'SELECT \'ISO\' AS "group", '
            'MLB_YEAR AS "key", '
            'ISOLATED_POWER AS "value" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '
            'ORDER BY "key" '
        )
        return pd.read_sql(sql, con=self.conn, params=params).to_json(orient='records')



    def get_player_data_all(self, player_id):
        params = [player_id]
        sql = (
            'SELECT ROW_NUMBER() OVER() "ID", '
            'MLB_YEAR AS "Year", '
            'PLAYER_AGE AS "Age", '
            'B_GAME AS "Games", '
            'B_TOTAL_PA AS "Plate Appearances", '
            'B_AB AS "At Bats", '
            'B_TOTAL_HITS AS "Hits", '
            'B_SINGLE AS "Singles", ' 
            'B_DOUBLE AS "Doubles", ' 
            'B_TRIPLE AS "Triples", ' 
            'B_HOME_RUN AS "Home Runs", '
            'B_RBI AS "Runs Batted In", '
            'B_STRIKEOUT AS "Strikeouts", '
            'B_WALK AS "Walks", ' 
            'XWOBA AS "xwOBA", '
            'BATTING_AVG AS "Batting AVG", '
            'ON_BASE_PERCENT AS "On Base Perc", '
            'SLG_PERCENT AS "Slugging Perc", '
            'ON_BASE_PLUS_SLG AS "On Base Plus SLG", '
            'ISOLATED_POWER AS "Isolated Power", '
            'B_TOTAL_BASES AS "Total Bases" '
            'FROM MLBSTATS '
            'WHERE PLAYER_ID = ? '
            'ORDER BY MLB_YEAR'
        )
        return pd.read_sql(sql, con=self.conn, params=params).to_json(orient='records')



    def get_all_predicted_data(self):
        sql = (
            'WITH Q1 AS '
                '(select p.PLAYER_ID, '
                'p.FIRST_NAME, '
                'p.LAST_NAME, '
                'm.XWOBA, '
                'pp.XWOBA_PREDICTED, '
                'pp.rsquared, '
                'pp.model '
                'from player_predictions pp '
                'inner join players p on p.player_id = pp.player_id '
                'inner join mlbstats m on m.player_id = p.player_id '
                'WHERE m.mlb_year = \'2021\' '
                'AND pp.mlb_year = \'2021\' '
            '), Q2 AS '
                '(SELECT PLAYER_ID, '
                'XWOBA_PREDICTED, '
                'RSQUARED, '
                'MODEL '
                'FROM player_predictions '
                'WHERE mlb_year = \'2022\' '
            ') '
            'SELECT '
            'Q1.PLAYER_ID AS "id", '
            'Q1.first_name AS "First Name", '
            'Q1.last_name AS "Last Name", '
            'Q1.XWOBA AS "2021 Actual xwOBA", '
            'CAST(ROUND(Q1.XWOBA_PREDICTED, 3) AS DECIMAL(6,3)) AS "2021 Predicted xwOBA", '
            'CAST(ROUND(Q2.XWOBA_PREDICTED, 3) AS DECIMAL(6,3)) AS "2022 Predicted xwOBA" '
            'FROM Q1 '
            'LEFT JOIN Q2 ON Q1.PLAYER_ID = Q2.PLAYER_ID '
            'ORDER BY Q1.XWOBA DESC'
        )
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



    def update_xgb_rsquared(self, model_type, year):
        params = [model_type, year, model_type]
        sql = (
            'UPDATE PLAYER_PREDICTIONS '
            'SET RSQUARED = (SELECT rsquared from XGBOOST_SCORES where model_type = ?) '
            'WHERE MLB_YEAR = ? and MODEL = ?'
        )
        self.engine.execute(sql, con=self.conn, *params)


    def get_prod_model_info(self):
        sql = (
            'SELECT n_estimators, '
            'subsample, '
            'max_depth, '
            'CAST(ROUND(learning_rate, 1) AS DECIMAL(6,3)) as learning_rate, '
            'gamma, '
            'reg_alpha, '
            'reg_lambda, '
            'training, '
            'mean_cv, '
            'kfold_cv_avg, '
            'mse, '
            'rsquared, '
            'explained_var '
            'from xgboost_hyperparams '
            'inner join xgboost_scores on xgboost_scores.model_type = xgboost_hyperparams.model_type '
            'where xgboost_hyperparams.model_type = \'prod_model\''
        )
        return pd.read_sql(sql, con=self.conn).to_json(orient='records')


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