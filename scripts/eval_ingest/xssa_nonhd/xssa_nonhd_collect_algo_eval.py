#!/usr/bin/env python3
"""
Collect algo_eval CSVs and plot RMSE statistics.

This script:
1. Collects 'algo_eval' CSV files from 8 RaFTS runs (plus Run 0).
   - For Run 0, it specifically filters for the 'rf' (Random Forest) algorithm.
2. Aggregates them into a single CSV.
3. Generates 3 plots (one per metric) showing the RMSE statistic.
4. Generates 3 box plots (one per metric) showing Mean Residuals +/- StdDev.

Author: NOAA-OWP/RaFTS User
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# -------------------------
# Config (edit if needed)
# -------------------------
output_root = Path("/home/jac/noaa/regionalization/data/output")
vis_dir     = output_root / "data_visualizations"
out_dir     = output_root / "analysis" / "Comparisons_NoNHD"

dataset_base = "juliemai-xSSA_nonhd"
# Runs 0-8. Run 0 comes from a different folder structure.
runs         = [0, 1, 2, 3, 4, 5, 6, 7, 8]
metrics      = ["KGE", "NSE", "RMSE"]


# -------------------------
# Helper
# -------------------------
def safe_read_algo_eval(path: Path):
    """Read an algo_eval CSV, dropping any unnamed index column. Return None on error."""
    if not path.exists():
        print(f"[WARN] Missing: {path}")
        return None
    try:
        df = pd.read_csv(path)
        # Clean column names (strip whitespace) to handle "algorithm " vs "algorithm"
        df.columns = df.columns.str.strip()
    except Exception as e:
        print(f"[WARN] Could not read {path}: {e}")
        return None

    # Drop any unnamed index column if present
    unnamed = [c for c in df.columns if str(c).startswith("Unnamed")]
    if unnamed:
        df = df.drop(columns=unnamed)

    return df


def generate_rmse_plots(df: pd.DataFrame, output_directory: Path):
    """
    Generates and saves 3 plots showing RMSE vs Run for each metric.
    Plots a single continuous line for the primary algorithm trend.
    """
    if "RMSE" not in df.columns:
        print("[ERROR] 'RMSE' column missing. Cannot generate plots.")
        return

    print("[INFO] Generating RMSE plots...")
    
    # Prepare data for plotting
    work_df = df.copy()

    # Normalize algorithm names if useful, though we will plot by Run
    if "algorithm" in work_df.columns:
        work_df["algorithm"] = work_df["algorithm"].fillna("xSSA").astype(str).str.strip().str.lower()
    
    # Sort by run to ensure the line connects correctly
    run_labels = sorted(work_df["run"].unique())

    for metric in metrics:
        subset = work_df[work_df["metric"] == metric].sort_values("run")
        
        if subset.empty:
            continue

        plt.figure(figsize=(10, 6))
        
        # Plot RMSE vs Run
        # We assume one valid point per run after filtering. 
        # If there are multiple, this will plot all of them.
        plt.plot(subset["run"], subset["RMSE"], 
                 marker='o', linestyle='-', linewidth=2, color='royalblue', label="xSSA/RF")

        plt.title(f"{metric} Model Performance: RMSE by Run", fontsize=14, fontweight='bold')
        plt.xlabel("Run Number", fontsize=12)
        plt.ylabel("RMSE (Residuals)", fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(run_labels)
        
        plot_path = output_directory / f"RMSE_Trend_{metric}.png"
        plt.savefig(plot_path, bbox_inches='tight', dpi=150)
        plt.close()
        print(f"  [SAVED] {plot_path}")


def generate_residual_boxplots(df: pd.DataFrame, output_directory: Path):
    """
    Generates and saves 3 box plots showing Mean Residual distributions.
    """
    mean_col = "AvgResid"
    std_col  = "SDResid"
    
    missing = [c for c in [mean_col, std_col] if c not in df.columns]
    if missing:
        print(f"[WARN] Missing columns for box plots: {missing}. Skipping.")
        return

    print("[INFO] Generating Residual Box Plots...")

    # Prepare DataFrame
    work_df = df.copy()
    work_df = work_df.sort_values("run")

    for metric in metrics:
        subset = work_df[work_df["metric"] == metric]
        if subset.empty:
            continue

        stats_list = []
        labels = []
        
        # Iterate through sorted runs
        for run_num in sorted(subset["run"].unique()):
            run_rows = subset[subset["run"] == run_num]
            
            for _, row in run_rows.iterrows():
                mu = row[mean_col]
                sigma = row[std_col]
                
                # Standard label is just the Run number
                label = str(int(run_num))

                stats_list.append({
                    "label": label,
                    "med": mu,
                    "q1": mu - sigma,
                    "q3": mu + sigma,
                    "whislo": mu - (2 * sigma),
                    "whishi": mu + (2 * sigma),
                    "fliers": []
                })

        if not stats_list:
            continue

        fig, ax = plt.subplots(figsize=(10, 6))
        
        ax.bxp(stats_list, showfliers=False, patch_artist=True,
               boxprops=dict(facecolor='lightblue', color='royalblue'),
               medianprops=dict(color='red', linewidth=2),
               whiskerprops=dict(color='royalblue', linewidth=1.5),
               capprops=dict(color='royalblue', linewidth=1.5))

        ax.set_title(f"{metric}: Mean Residual Distribution (Box = Mean ± 1 Std)", fontsize=14, fontweight='bold')
        ax.set_xlabel("Run Number", fontsize=12)
        ax.set_ylabel("Mean Residual", fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.7)

        plot_path = output_directory / f"BoxPlot_Residuals_{metric}.png"
        plt.savefig(plot_path, bbox_inches='tight', dpi=150)
        plt.close()
        print(f"  [SAVED] {plot_path}")


# -------------------------
# Main
# -------------------------
def main() -> None:
    print("[INFO] Collecting algo_eval CSVs")
    frames = []

    for run in runs:
        # Determine the tag and folder logic
        if run == 0:
            # Run 0: Folder is "juliemai-xSSA" (no _nonhd, no number)
            tag = "juliemai-xSSA"
        else:
            # Runs 1-8: Folder is "juliemai-xSSA_nonhd_XX"
            tag = f"{dataset_base}_{run:02d}"

        for metric in metrics:
            csv_path = vis_dir / tag / f"algo_eval_{tag}_{metric}.csv"

            df = safe_read_algo_eval(csv_path)
            if df is None or df.empty:
                continue

            # ---------------------------------------------------------
            # SPECIAL HANDLING FOR RUN 0: Filter for "rf" only
            # ---------------------------------------------------------
            if run == 0 and "algorithm" in df.columns:
                # Keep only Random Forest ('rf')
                # Exclude 'mlp' or others
                df = df[df["algorithm"].astype(str).str.strip().str.lower() == "rf"]
                
                if df.empty:
                    print(f"[WARN] Run 0 (juliemai-xSSA) filtered to empty! (No 'rf' found in {csv_path})")
                    continue

            # Standardize identifier columns
            if "dataset" not in df.columns:
                df["dataset"] = tag
            if "tag" not in df.columns:
                df["tag"] = tag
            if "run" not in df.columns:
                df["run"] = run
            
            # Ensure metric column is set and upper-case
            if "metric" not in df.columns:
                df["metric"] = metric
            else:
                df["metric"] = df["metric"].astype(str).str.upper()

            frames.append(df)

    if not frames:
        print("[WARN] No algo_eval CSVs found.")
        return

    algo_eval_all = pd.concat(frames, ignore_index=True)

    # Sort
    sort_cols = [c for c in ["run", "metric", "algorithm"] if c in algo_eval_all.columns]
    if sort_cols:
        algo_eval_all = algo_eval_all.sort_values(sort_cols).reset_index(drop=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "algo_eval_all_collected.csv"
    algo_eval_all.to_csv(out_path, index=False)

    print("[OK] Wrote:", out_path)
    print("      rows:", len(algo_eval_all))
    
    # Generate graphics
    generate_rmse_plots(algo_eval_all, out_dir)
    generate_residual_boxplots(algo_eval_all, out_dir)


if __name__ == "__main__":
    main()