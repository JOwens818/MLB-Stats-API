import json
import os
import logging
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from mods.MLBStatPredictor import MLBStatPredictor
from mods.DB2Connect import DB2Connect
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB2_CREDS = {
    "user": os.environ.get("DB2_USER"),
    "pw": os.environ.get("DB2_PW"),
    "host": os.environ.get("DB2_HOST"),
    "port": os.environ.get("DB2_PORT"),
    "db": os.environ.get("DB2_DB")
}

app = Flask(__name__)
CORS(app)
DB2 = DB2Connect(DB2_CREDS)
MLB_PREDICTOR = MLBStatPredictor('xwoba')

# Setup Logging
gu_logger = logging.getLogger('gunicorn.error')
app.logger.handlers = gu_logger.handlers
app.logger.setLevel(gu_logger.level)


@app.route('/api/create-xgb-model', methods=['POST'])
def create_xgb_model():
    try:
        req = json.loads(request.data)
        model_type = req["model_type"]
        year = req["year"]
        hyper_params = {
            "n_estimators": req["n_estimators"],
            "subsample": req["subsample"],
            "max_depth": req["max_depth"],
            "learning_rate": req["learning_rate"],
            "gamma": req["gamma"],
            "reg_alpha": req["reg_alpha"],
            "reg_lambda": req["reg_lambda"]
        }

        app.logger.info(f"Getting data from DB2 - 2015 - {year}")
        df = DB2.get_all_data('2015', str(int(year) - 1))
        
        app.logger.info("Creating and saving " + model_type)
        scores = MLB_PREDICTOR.create_xgboost_model(df, model_type, hyper_params)

        #app.logger.info("Saving scores and hyperparameters to DB2")
        #DB2.save_xgb_scores(scores, model_type)
        #DB2.save_xgb_hyperparams(hyper_params, model_type)
        
        response_val = {
            "n_estimators": hyper_params["n_estimators"],
            "subsample": hyper_params["subsample"],
            "max_depth": hyper_params["max_depth"],
            "learning_rate": hyper_params["learning_rate"],
            "gamma": hyper_params["gamma"],
            "reg_alpha": hyper_params["reg_alpha"],
            "reg_lambda": hyper_params["reg_lambda"],
            "training": scores["training"],
            "mean_cv": scores["mean_cv"],
            "kfold_cv_avg": scores["kf_cv"],
            "mse": scores["mse"],
            "rsquared": scores["r2s"],
            "explained_var": scores["explained_var"]
        }

        return Response(json.dumps(response_val), status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/xgb-model-predict', methods=['POST'])
def xgb_model_predict():
    try:
        req = json.loads(request.data)
        model_type = req["model_type"]
        prediction = MLB_PREDICTOR.xgboost_predict(req, model_type)
        resp = {
            "predicted xwOBA": prediction
        }
        return Response(json.dumps(resp), status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/predict-stats', methods=['POST'])
def predict_stats():
    try:
        req = json.loads(request.data)
        year = req["year"]
        model_type = req["model_type"]
        xgb_only = req["xgb_only"]

        app.logger.info("Getting DB2 data")
        df = DB2.get_all_data('2015', str(int(year) - 1))

        app.logger.info("Running prediction method")
        predictions = MLB_PREDICTOR.generate_predictions(df, int(year), model_type, xgb_only)
        
        app.logger.info("Saving data in DB2")
        DB2.delete_model_predictions_by_year(int(year))
        DB2.append_to_table(predictions, "player_predictions", "append")
        DB2.update_xgb_rsquared(model_type, int(year))

        return Response(status=201, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/predicted-stats-all', methods=['GET'])
def get_predicted_stats_all():
    try:
        stats = DB2.get_all_predicted_data()
        return Response(stats, status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/charts/histogram', methods=['GET'])
def get_histogram_data():
    try:
        hist_data = DB2.get_histogram_data(request.args['field'])
        if not hist_data:
            raise Exception("Requested field does not exist")

        return Response(hist_data, status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/charts/histogram-stats', methods=['GET'])
def get_histogram_stats():
    try:
        hist_stats = DB2.get_histogram_stats(request.args['field'])
        if not hist_stats:
            raise Exception("Requested field does not exist")

        return Response(hist_stats, status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/charts/scatter', methods=['GET'])
def get_scatter_data():
    try:
        scatter_data = DB2.get_scatter_data(request.args['field'])
        if not scatter_data:
            raise Exception("Requested field does not exist")

        return Response(scatter_data, status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/charts/radar', methods=['GET'])
def get_radar_player_data():
    try:
        player_id = request.args['playerid']
        radar_data = DB2.get_radar_player_data(player_id)
        return Response(radar_data, status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/charts/line', methods=['GET'])
def get_line_player_data():
    try:
        player_id = request.args['playerid']
        line_data = DB2.get_line_player_data(player_id)
        return Response(line_data, status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/player-stats', methods=['GET'])
def get_player_stats_all():
    try:
        player_id = request.args['playerid']
        line_data = DB2.get_player_data_all(player_id)
        return Response(line_data, status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')



@app.route('/api/prod-model-info', methods=['GET'])
def get_prod_model_info():
    try:
        prod_info = DB2.get_prod_model_info()
        return Response(prod_info, status=200, mimetype='application/json')

    except Exception as e:
        app.logger.error(e)
        err_resp = {
            "errorMsg": repr(e)
        }
        return Response(json.dumps(err_resp), status=400, mimetype='application/json')