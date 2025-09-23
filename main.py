import argparse
import json
from source.recommend_compass import recommend
import source.settings as settings

# -----------------------------
# CLI Parser
# -----------------------------
def _parse_args():
    """
    Parse command-line arguments for the recommendation engine.
    """
    p = argparse.ArgumentParser(
        description="Recommend events, factors, and variables for a given company and report section(s)."
    )

    # Required identifiers
    p.add_argument("--company", required=True,
                   help="Company name (matches report.company_name, case-insensitive).")
    p.add_argument("--sections", nargs="+", required=True,
                   help="One or more section names (exact match recommended).")

    # Top-K controls
    p.add_argument("--k", type=int, default=None,
                   help="Convenience: set the same Top-K for variables, factors, and events.")
    p.add_argument("--k-var", type=int, default=None,
                   help="Top-K for variables. Overrides --k when provided.")
    p.add_argument("--k-factor", type=int, default=None,
                   help="Top-K for factors. Overrides --k when provided.")
    p.add_argument("--k-event", type=int, default=None,
                   help="Top-K for events. Overrides --k when provided.")

    # Time/window constraints
    p.add_argument("--year-min", type=int, default=None, help="Minimum year (inclusive).")
    p.add_argument("--year-max", type=int, default=None, help="Maximum year (inclusive).")
    p.add_argument("--report-limit", type=int, default=None,
                   help="Max number of reports (most recent first).")

    # Output
    p.add_argument("--out", default=None, help="Optional JSON output path.")

    # Hybrid weights (keep defaults aligned with settings)
    p.add_argument("--w-comp", type=float, default=settings.weight_company,
                   help=f"Hybrid weight for company rank (default: {settings.weight_company}).")
    p.add_argument("--w-glob", type=float, default=settings.weight_global,
                   help=f"Hybrid weight for global rank (default: {settings.weight_global}).")
    p.add_argument("--w-freq", type=float, default=settings.weight_frequency,
                   help=f"Hybrid weight for normalized frequency (default: {settings.weight_frequency}).")
    p.add_argument("--both-bonus", type=float, default=settings.both_bonus,
                   help=f"Bonus if appears in both company & global (default: {settings.both_bonus}).")

    return p.parse_args()

# -----------------------------
# Main
# -----------------------------
def main():
    """
    Main entry point: parse CLI args, call recommend(), and handle output.
    """
    args = _parse_args()

    # Resolve Top-K values with precedence: specific flag > --k > function default
    k_all = args.k
    k_var = args.k_var if args.k_var is not None else (k_all if k_all is not None else 8)
    k_factor = args.k_factor if args.k_factor is not None else (k_all if k_all is not None else 6)
    k_event = args.k_event if args.k_event is not None else (k_all if k_all is not None else 6)

    results = recommend(
        company_name=args.company,
        section_names=args.sections,
        k_var=k_var,
        k_factor=k_factor,
        k_event=k_event,
        year_min=args.year_min,
        year_max=args.year_max,
        report_limit=args.report_limit,
        w_comp=args.w_comp,
        w_glob=args.w_glob,
        w_freq=args.w_freq,
        both_bonus=args.both_bonus,
    )

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"Results saved to {args.out}")
    else:
        print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
