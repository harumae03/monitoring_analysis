import pandas as pd
import numpy as np
from pathlib import Path
import sys


STD_DEV_THRESHOLD = 3.0
CONSECUTIVE_MINUTES_THRESHOLD = 10
BASELINE_ZERO_THRESHOLD = 200 # at least this many logins normally for zero to be alarming
# small number to treat standard deviation as effectively zero
STD_DEV_EPSILON = 1e-6


def load_and_prepare_data(filepath: Path) -> pd.DataFrame | None:
    if not filepath.exists():
        print(f"Error: File not found at {filepath}")
        return None
    try:
        df = pd.read_csv(filepath)
        if df.shape[1] != 2:
             print(f"Error: CSV file {filepath} should have 2 columns (time, measurement_value). Found {df.shape[1]}.")
             return None
        df.columns = ['time', 'measurement_value']
        df['measurement_value'] = pd.to_numeric(df['measurement_value'], errors='coerce')

        if df['measurement_value'].isnull().any():
            print(f"Warning: Non-numeric values found in {filepath}, replacing with 0.")
            df['measurement_value'].fillna(0, inplace=True)

        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        df.sort_index(inplace=True)
        print(f"Successfully loaded and prepared data from {filepath}")
        return df
    except Exception as e:
        print(f"Error loading or preparing data from {filepath}: {e}")
        return None

def calculate_baseline_stats(baseline_df: pd.DataFrame) -> pd.DataFrame | None:
    if baseline_df is None:
        return None
    try:
        df_copy = baseline_df.copy()
        df_copy['minute_of_week'] = (
            df_copy.index.dayofweek * 24 * 60 +
            df_copy.index.hour * 60 +
            df_copy.index.minute
        )
        baseline_stats = df_copy.groupby('minute_of_week')['measurement_value'].agg(['mean', 'std'])
        baseline_stats['std'] = baseline_stats['std'].fillna(0)
        print("Calculated baseline statistics (mean, std dev) per minute-of-the-week.")
        return baseline_stats
    except Exception as e:
        print(f"Error calculating baseline statistics: {e}")
        return None

def detect_anomalies(measured_df: pd.DataFrame, baseline_stats: pd.DataFrame) -> pd.DataFrame | None:
    # detects anomalies by comparing measured data to baseline data.
    if measured_df is None or baseline_stats is None:
        return None
    try:
        df_copy = measured_df.copy()
        df_copy['minute_of_week'] = (
            df_copy.index.dayofweek * 24 * 60 +
            df_copy.index.hour * 60 +
            df_copy.index.minute
        )

        # ensures the index is named 'time' before merging if it got lost
        if df_copy.index.name != 'time':
             df_copy.index.name = 'time'


        merged_df = pd.merge(
            df_copy,
            baseline_stats,
            left_on='minute_of_week',
            right_index=True, # merges on the index of baseline_stats
            how='left',
            suffixes=('', '_baseline')
        )


        merged_df['mean'] = merged_df['mean'].fillna(0)
        merged_df['std'] = merged_df['std'].fillna(0)

        merged_df['upper_bound'] = merged_df['mean'] + STD_DEV_THRESHOLD * merged_df['std']
        merged_df['lower_bound'] = np.maximum(0, merged_df['mean'] - STD_DEV_THRESHOLD * merged_df['std'])


        # deviation anomaly: Value significantly deviates, but ONLY if std is not effectively zero
        deviation_anomaly = (
            ((merged_df['measurement_value'] < merged_df['lower_bound']) | \
             (merged_df['measurement_value'] > merged_df['upper_bound'])) & \
            (merged_df['std'] > STD_DEV_EPSILON) # Check against small epsilon
        )

        #  zero anomaly: Measured value is zero when baseline expects activity
        zero_anomaly = (merged_df['measurement_value'] == 0) & \
                       (merged_df['mean'] >= BASELINE_ZERO_THRESHOLD)


        merged_df['is_anomaly'] = deviation_anomaly | zero_anomaly


        merged_df['anomaly_type'] = 'Normal' # default value
        # zero anomaly is nr 1 priority
        merged_df.loc[zero_anomaly, 'anomaly_type'] = 'Zero'
        # deviation anomalies come second
        merged_df.loc[deviation_anomaly & (merged_df['anomaly_type'] == 'Normal') & (merged_df['measurement_value'] < merged_df['lower_bound']), 'anomaly_type'] = 'Low'
        merged_df.loc[deviation_anomaly & (merged_df['anomaly_type'] == 'Normal') & (merged_df['measurement_value'] > merged_df['upper_bound']), 'anomaly_type'] = 'High'


        print("Anomaly detection complete.")
        if not isinstance(merged_df.index, pd.DatetimeIndex):
             print("Warning: Index might have been lost during processing.")
        return merged_df

    except Exception as e:
        print(f"Error detecting anomalies: {e}")
        return None


def monitor_and_alert(anomalies_df: pd.DataFrame):

    if anomalies_df is None or not isinstance(anomalies_df.index, pd.DatetimeIndex):
        print("Cannot monitor: Anomaly data is missing or index is not a DatetimeIndex.")
        return

    print(f"\n--- Monitoring Started ---")
    print(f"Alerting on issues lasting >= {CONSECUTIVE_MINUTES_THRESHOLD} minutes.")
    print(f"Deviation threshold: {STD_DEV_THRESHOLD} std deviations from baseline mean (ignored if baseline std < {STD_DEV_EPSILON}).")
    print(f"Zero threshold: Alerts if logins drop to 0 when baseline mean is >= {BASELINE_ZERO_THRESHOLD}.\n")

    alert_active = False
    anomaly_streak = 0
    normal_streak = 0
    alert_start_timestamp = None
    initial_anomaly_type = None

    for row_tuple in anomalies_df.itertuples():
        current_timestamp = row_tuple.Index
        is_anomaly_now = row_tuple.is_anomaly
        current_anomaly_type = row_tuple.anomaly_type

        if is_anomaly_now:
            anomaly_streak += 1
            normal_streak = 0
            if anomaly_streak >= CONSECUTIVE_MINUTES_THRESHOLD and not alert_active:
                try:
                    current_pos = anomalies_df.index.get_loc(current_timestamp)
                    start_pos = max(0, current_pos - (CONSECUTIVE_MINUTES_THRESHOLD - 1))
                    alert_start_timestamp = anomalies_df.index[start_pos]
                    first_anomaly_row_data = anomalies_df.iloc[start_pos]
                    initial_anomaly_type = first_anomaly_row_data['anomaly_type']

                    print(f"--- ALERT STARTED ---")
                    print(f"Detected at Time: {current_timestamp}")
                    print(f"Estimated Start:  {alert_start_timestamp} (Issue lasting >= {CONSECUTIVE_MINUTES_THRESHOLD} mins)")
                    print(f"Initial Reason:   Anomaly Type '{initial_anomaly_type}'")
                    print(f"  Initial Value:  {first_anomaly_row_data['measurement_value']:.2f} at {alert_start_timestamp}")
                    print(f"  Current Value:  {row_tuple.measurement_value:.2f} at {current_timestamp}")
                    print(f"  Baseline Mean:  {row_tuple.mean:.2f} (this minute)")
                    print(f"  Expected Range: [{row_tuple.lower_bound:.2f} - {row_tuple.upper_bound:.2f}] (if std > {STD_DEV_EPSILON})")
                    print("-" * 20)
                    alert_active = True

                except Exception as e:
                     print(f"Error during alert start logic at {current_timestamp}: {e}")
                     alert_active = False
                     alert_start_timestamp = None
                     initial_anomaly_type = None

        else: # not an anomaly
            normal_streak += 1
            if anomaly_streak > 0:
                anomaly_streak = 0
            if normal_streak >= CONSECUTIVE_MINUTES_THRESHOLD and alert_active:
                try:
                    current_pos = anomalies_df.index.get_loc(current_timestamp)
                    resolve_pos = max(0, current_pos - (CONSECUTIVE_MINUTES_THRESHOLD - 1))
                    resolve_time_estimate = anomalies_df.index[resolve_pos]

                    print(f"--- ALERT RESOLVED ---")
                    print(f"Detected at Time: {current_timestamp}")
                    print(f"Estimated Resolve: {resolve_time_estimate} (Normal >= {CONSECUTIVE_MINUTES_THRESHOLD} mins)")
                    print(f"Original Alert Start: {alert_start_timestamp} (Reason: '{initial_anomaly_type}')")
                    print(f"  Current Value: {row_tuple.measurement_value:.2f}")
                    print(f"  Baseline Mean: {row_tuple.mean:.2f}")
                    print("-" * 20)
                    alert_active = False
                    alert_start_timestamp = None
                    initial_anomaly_type = None
                except Exception as e:
                    print(f"Error during alert resolve logic at {current_timestamp}: {e}")
                    alert_active = False
                    alert_start_timestamp = None
                    initial_anomaly_type = None

    print("\n--- Monitoring Finished ---")
    if alert_active:
        print(f"Warning: Monitoring finished while an alert starting around {alert_start_timestamp} (Reason: '{initial_anomaly_type}') was still active.")


def main():

    baseline_path_str = input("Enter the name of the baseline CSV file (e.g., baseline 1.csv): ")
    measured_path_str = input("Enter the name of the measured data CSV file (e.g., measured 1.csv): ")

    baseline_file = Path(baseline_path_str.strip())
    measured_file = Path(measured_path_str.strip())

    print("\nLoading baseline data...")
    baseline_df = load_and_prepare_data(baseline_file)
    if baseline_df is None: return

    print("\nCalculating baseline statistics...")
    baseline_stats = calculate_baseline_stats(baseline_df)
    if baseline_stats is None: return

    print("\nLoading measured data...")
    measured_df = load_and_prepare_data(measured_file)
    if measured_df is None: return

    print("\nDetecting anomalies...")
    anomalies_df = detect_anomalies(measured_df, baseline_stats)

    monitor_and_alert(anomalies_df)

if __name__ == "__main__":
    main()