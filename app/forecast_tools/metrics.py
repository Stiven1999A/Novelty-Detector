"""DETECTOR-DE-NOVEDADES/forecast_tools/metrics.py"""
import numpy as np

def mas(n: int, y_true: float, y_pred:float, last_metric: dict) -> float:
    """
    Calculate the moving average of the absolute error.

    This function computes the moving average of the absolute error 
    between the true values and the predicted values.

    Parameters:
    n (int): The current time step or index.
    y_true (float): The true value at the current time step.
    y_pred (float): The predicted value at the current time step.
    last_metric (float): The moving average of the absolute error from the previous time step.

    Returns:
    float: The updated moving average of the absolute error.
    """
    metric = (n - 1) * last_metric / n + abs(y_true - y_pred) / n
    return metric

def mse(n: int, y_true: float, y_pred:float, last_metric: dict) -> float:
    """
    Calculate the mean squared error (MSE) incrementally.

    Parameters:
    n (int): The number of observations.
    y_true (float): The true value.
    y_pred (float): The predicted value.
    last_metric (float): The previous MSE value.

    Returns:
    float: The updated MSE value.
    """
    metric = (n - 1) * last_metric / n + (y_true - y_pred) ** 2 / n
    return metric

def rmse(n: int, y_true: float, y_pred:float, last_metric: dict) -> float:
    """
    Calculate the Root Mean Square Error (RMSE) incrementally.

    Parameters:
    n (int): The number of observations.
    y_true (float): The true value.
    y_pred (float): The predicted value.
    last_metric (float): The RMSE value from the previous calculation.

    Returns:
    float: The updated RMSE value.
    """
    metric = np.sqrt((n - 1) * (last_metric ** 2) / n + ((y_true - y_pred) ** 2 / n))
    return metric

def mape(n: int, y_true: float, y_pred:float, last_metric: dict) -> float:
    """
    Calculate the Mean Absolute Percentage Error (MAPE) with incremental update.

    Parameters:
    n (int): The number of observations.
    y_true (float): The actual value.
    y_pred (float): The predicted value.
    last_metric (float): The MAPE value from the previous calculation.

    Returns:
    float: The updated MAPE value.
    """
    metric = (n - 1) * last_metric / n + (100 / n) * abs((y_true - y_pred) / y_true)
    return metric

def smape(n: int, y_true: float, y_pred:float, last_metric: dict) -> float:
    """
    Calculate the Symmetric Mean Absolute Percentage Error (sMAPE) for a given 
    set of true and predicted values.

    sMAPE is a measure of prediction accuracy of a forecasting method in statistics. 
    It is an alternative to the Mean Absolute Percentage Error (MAPE) and is used to 
    avoid the problem of having undefined values when the true value is zero.

    Args:
        n (int): The number of observations.
        y_true (float): The actual value.
        y_pred (float): The predicted value.
        last_metric (float): The sMAPE value from the previous calculation.

    Returns:
        float: The updated sMAPE value.
    """
    metric = ((n - 1) / n) * last_metric + (200 / n) * (abs(y_true - y_pred) / (abs(y_true) + abs(y_pred)))
    return metric

def metrics(n: int, y_true: float, y_pred: float, last_metrics: dict) -> dict:
    """
    Calculate various error metrics for a given set of true and predicted values.

    Args:
        n (int): The current number of observations.
        y_true (float): The actual value.
        y_pred (float): The predicted value.
        last_metrics (dict): A dictionary containing the last calculated values of the metrics.

    Returns:
        dict: A dictionary containing the updated values of the following metrics:
            - "MAE": Mean Absolute Error
            - "MSE": Mean Squared Error
            - "RMSE": Root Mean Squared Error
            - "MAPE": Mean Absolute Percentage Error
            - "sMAPE": Symmetric Mean Absolute Percentage Error
    """
    return {
        "MAE": mas(n, y_true, y_pred, last_metrics["MAE"]),
        "MSE": mse(n, y_true, y_pred, last_metrics["MSE"]),
        "RMSE": rmse(n, y_true, y_pred, last_metrics["RMSE"]),
        "MAPE": mape(n, y_true, y_pred, last_metrics["MAPE"]),
        "sMAPE": smape(n, y_true, y_pred, last_metrics["sMAPE"])
    }
