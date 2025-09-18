import argparse
from exp.linear_regression import LinearRegressionExp
from exp.xgboost_regression import XGBoostExp
from exp.arima import ARIMAExp

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', type=str, required=True,
                        help='Model to run: linear_regression | xgboost | arima')
    parser.add_argument('--task', type=str, required=True,
                        help='Task to perform: nowcasting | auto-regression | hyperparams_search')
    parser.add_argument('--data_path', type=str, required=True, help='Path to the dataset pickle file')
    parser.add_argument('--save_format', type=str, default='json', choices=['json', 'csv', 'all'],
                        help='Format to save prediction and metric results')
    parser.add_argument('--results_path', type=str, default='./results', help='Root directory to save results')
    parser.add_argument('--checkpoints_path', type=str, default='./checkpoints', help='Where to save model checkpoints')
    parser.add_argument('--search', action='store_true', help='Whether to use saved hyperparameters (if available)')
    parser.add_argument('--eval_metric', type=str, default='rmse',
                        help='Metric used to select best hyperparams: rmse, mse, mae')

    args = parser.parse_args()

    if args.model == 'linear_regression':
        exp = LinearRegressionExp(
            data_path=args.data_path,
            model_name=args.model,
            results_path=args.results_path,
            checkpoints_path=args.checkpoints_path,
            save_format=args.save_format
        )
        exp.train()
        exp.test(task=args.task)

    elif args.model == 'xgboost':
        exp = XGBoostExp(
            data_path=args.data_path,
            model_name=args.model,
            results_path=args.results_path,
            checkpoints_path=args.checkpoints_path,
            save_format=args.save_format,
            eval_metric=args.eval_metric
        )

        if args.task == 'hyperparams_search':
            exp.load_data()
            param_grid = {
                'n_estimators': [25, 50, 100],
                'max_depth': [3, 5, 7],
                'learning_rate': [1e-4, 1e-3, 1e-2],
            }
            exp.hyperparams_search(param_grid)
            return

        exp.load_data()
        exp.train()
        exp.test(task=args.task)

    elif args.model == 'arima':
        exp = ARIMAExp(
            data_path=args.data_path,
            model_name=args.model,
            results_path=args.results_path,
            checkpoints_path=args.checkpoints_path,
            save_format=args.save_format
        )

        if args.task == 'hyperparams_search':
            raise NotImplementedError("ARIMA does not support hyperparameter search.")

        exp.load_data()
        exp.test(task=args.task)

    else:
        raise NotImplementedError(f"Model '{args.model}' not supported.")

if __name__ == '__main__':
    main()
