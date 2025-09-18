
# GDP Time Series Forecasting Project

This project contains multiple machine learning models (Linear Regression, XGBoost, ARIMA) to forecast GDP time series data. The models can be trained, tested, and optimized using different tasks and hyperparameters.

## Project Structure

```
.
├── exp/
│   ├── linear_regression.py
│   ├── xgboost_regression.py
│   └── arima.py
├── data/
│   ├── ts_datasets/
│   │   └── all_datasets.pkl
├── results/             # Model result storage
├── checkpoints/         # Model checkpoint storage
├── hyperparams/         # Hyperparameter search results
├── utils/               # Utility scripts
├── ml_exp.py            # Main script to execute models
└── README.md         # Documentation
└── scripts/             # Bash scripts for running tasks
    ├── linear_regression_run.sh
    ├── xgboost_run.sh
    └── arima_run.sh
```

## Models Supported

### 1. **Linear Regression** (`linear_regression.py`):
   - A simple linear regression model for time series forecasting.

### 2. **XGBoost** (`xgboost_regression.py`):
   - A gradient boosting framework for regression tasks.
   - Supports hyperparameter optimization through grid search.

### 3. **ARIMA** (`arima.py`):
   - AutoRegressive Integrated Moving Average model for time series forecasting.
   - Currently only supports testing (does not require training).

## Installation

Install the necessary dependencies using:

```bash
pip install -r requirements_ml.txt
```

`requirements_ml.txt` should include the following libraries (if not installed):

```
numpy
pandas
scikit-learn
xgboost
statsmodels
pmdarima
```

## Usage

### 1. **Training and Testing Models**

To run any of the models with a specific task (nowcasting, auto-regression), use the following command:

```bash
python ml_exp.py --model <model_name> --task <task_name> --data_path <path_to_data> --save_format <save_format>
```

- `<model_name>`: `linear_regression`, `xgboost`, or `arima`.
- `<task_name>`: `nowcasting`, `auto-regression`, or `hyperparams_search`.
- `<save_format>`: `json`, `csv`, or `all` (both json and csv).
- `<path_to_data>`: Path to the `all_datasets.pkl` file.

### 2. **Hyperparameter Search (For XGBoost)**

To run a hyperparameter search for the XGBoost model, use:

```bash
python ml_exp.py --model xgboost --task hyperparams_search --data_path <path_to_data> --eval_metric <metric>
```

- `<eval_metric>`: `mse`, `rmse`, or `mae` for selecting the best hyperparameters.

### 3. **Testing ARIMA**

ARIMA doesn't require training, only testing. To run ARIMA, use:

```bash
python ml_exp.py --model arima --task <task_name> --data_path <path_to_data> --save_format <save_format>
```

- `<task_name>`: `nowcasting` or `auto-regression`.

### 4. **Aggregate Results**

The `aggregate_results.py` script helps to aggregate the results from multiple models and generate a final summary in Excel format. You can use this script to consolidate the evaluation metrics of different models (such as `arima`, `xgboost`, `linear_regression`) and countries (such as `USA`, `CHN`) into a single report.

To run this script, use the following command:

```bash
python aggregate_results.py --file_format <file_format> --results_base_path <results_base_path> --output_file <output_file> --task <task_name>
```

- `<file_format>`: `json` or `csv` (Choose the format of the saved results).
- `<results_base_path>`: Path to the `results/` directory where model results are stored.
- `<output_file>`: Path to the output Excel file where the aggregated results will be saved.
- `<task_name>`: The task used for the results (`auto-regression` or `nowcasting`).

**Example**:

```bash
python aggregate_results.py --file_format json --results_base_path ./results --output_file ./aggregated_results.xlsx --task auto-regression
```

### 5. **Testing ARIMA**

```bash
python ml_exp.py   --model arima   --task nowcasting   --data_path data/ts_datasets/all_datasets.pkl   --save_format csv
```

## Tasks

### 1. **Nowcasting**
- Predict the future values using the model and compare with actual values.

### 2. **Auto-regression**
- Predict the next value using the previous ones, iteratively, until the predicted sequence length matches the actual sequence.

### 3. **Hyperparameter Search**
- Automatically searches for the best hyperparameters for the selected model using grid search.
- The best hyperparameters are saved in the `hyperparams/` directory.

### 4. **Aggregate Results**
- This task consolidates the results of different models into a final summary table. 
- The results are stored in a MultiIndex format with countries as the first level and metrics as the second level.
- The output is saved in an Excel file.

## Result Storage

- **Results** are saved in `results/` for predictions and metrics.
- **Model checkpoints** are saved in `checkpoints/` for future use.

## Hyperparameter Search Results

The best hyperparameters found during search are saved in the `hyperparams/` folder in JSON format. The file contains:
- `best_params`: Best hyperparameters found.
- `metric_used`: The metric used for optimization (`rmse`, `mae`, or `mse`).
- `full_results`: A dictionary of all parameter combinations and their corresponding evaluation metrics.

## Example

### Example 1: Training and Testing XGBoost for Auto-regression

```bash
python ml_exp.py   --model xgboost   --task auto-regression   --data_path data/ts_datasets/all_datasets.pkl   --save_format all
```

```bash
python ml_exp.py   --model xgboost   --task nowcasting   --data_path data/ts_datasets/all_datasets.pkl   --save_format all
```

### Example 2: Hyperparameter Search for XGBoost

```bash
python ml_exp.py   --model xgboost   --task hyperparams_search   --data_path data/ts_datasets/all_datasets.pkl   --eval_metric rmse
```

### Example 3: Testing ARIMA for Nowcasting

```bash
python ml_exp.py   --model arima   --task nowcasting   --data_path data/ts_datasets/all_datasets.pkl   --save_format csv
```
```bash
python ml_exp.py   --model arima   --task auto-regression   --data_path data/ts_datasets/all_datasets.pkl   --save_format csv
```


# linear
```bash
python ml_exp.py   --model linear_regression   --task auto-regression   --data_path data/ts_datasets/all_datasets.pkl   --save_format csv
```

```bash
python ml_exp.py   --model linear_regression   --task nowcasting   --data_path data/ts_datasets/all_datasets.pkl   --save_format csv
```

```bash
python tsllm_exp.py --task nowcasting --data_path data/ts_datasets/all_datasets.pkl --save_format csv --train
```
```bash
python tsllm_exp.py --task auto-regression --data_path data/ts_datasets/all_datasets.pkl --save_format csv --train
```
```bash
python tsllm_exp.py --task nowcasting --data_path data/ts_datasets/all_datasets.pkl --save_format csv
```
```bash
python tsllm_exp.py --task auto-regression --data_path data/ts_datasets/all_datasets.pkl --save_format csv
```
## Bash Scripts

To simplify running tasks, we have prepared the following bash scripts under the `scripts/` folder:

### 1. **Run Linear Regression**

```bash
bash scripts/linear_regression_run.sh
```

### 2. **Run XGBoost** (with optional hyperparameter search)

```bash
bash scripts/xgboost_run.sh
```

### 3. **Run ARIMA**

```bash
ba   sh scripts/arima_run.sh
```

## Notes

1. **ARIMA** only works with the `test()` function as it doesn't need training.
2. For `XGBoost`, **hyperparameter search** can significantly improve model performance, especially for time series forecasting.

---

If you encounter any issues, please feel free to raise an issue or pull request.
