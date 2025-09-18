import argparse
import random
import numpy as np
import torch
from exp.time_llm import TimeLLMExp


def set_random_seed(seed):
    """Set random seed for reproducibility."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, required=True,
                        help='Path to all_datasets.pkl')
    parser.add_argument('--task', type=str, default='nowcasting',
                        choices=['nowcasting', 'auto-regression'],
                        help='Choose the testing task type')
    parser.add_argument('--save_format', type=str, default='json', choices=['json', 'csv', 'all'])
    parser.add_argument('--results_path', type=str, default='./results')
    parser.add_argument('--checkpoints_path', type=str, default='./checkpoints')
    parser.add_argument('--eval_metric', type=str, default='rmse')
    parser.add_argument('--gpu', type=int, default=0, help='Which GPU to use (e.g., 0 or 1)')
    parser.add_argument('--train', action='store_true', help='Whether to train the model')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')  # ✅ New seed arg

    args = parser.parse_args()

    # ✅ Set seed
    set_random_seed(args.seed)

    device = f'cuda:{args.gpu}' if torch.cuda.is_available() else 'cpu'

    llm_args = argparse.Namespace(
        task_name='short_term_forecast',
        model_id='test',
        model='TimeLLM',
        data='GDP',
        features='M',
        seq_len=10,
        label_len=0,
        pred_len=1,

        enc_in=1,
        dec_in=1,
        c_out=1,
        d_model=32,
        n_heads=8,
        e_layers=2,
        d_layers=1,
        d_ff=128,
        moving_avg=25,
        factor=1,
        dropout=0.1,
        embed='timeF',
        activation='gelu',
        output_attention=False,
        patch_len=5,
        stride=1,
        prompt_domain=0,
        # llm_model='LLAMA3_instruct',
        llm_model='LLAMA',
        llm_dim=4096,
        num_workers=0,
        itr=1,
        train_epochs=20,
        align_epochs=10,
        batch_size=16,
        eval_batch_size=16,
        patience=1000,
        learning_rate=1e-5,
        des='test',
        loss='MSE',
        lradj='type1',
        pct_start=0.2,
        use_amp=False,
        llm_layers=6,
        percent=100,
        description_add="GDP time series forecasting",
    )

    exp = TimeLLMExp(
        data_path=args.data_path,
        model_name='time_llm',
        results_path=args.results_path,
        checkpoints_path=args.checkpoints_path,
        save_format=args.save_format,
        eval_metric=args.eval_metric,
        device=device
    )

    exp.load_data()

    if args.train:
        exp.train(llm_args)

    exp.test(llm_args, task=args.task)


if __name__ == '__main__':
    main()
