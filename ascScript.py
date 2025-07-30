import can
import cantools
import pandas as pd

# Load the DBC file
dbc_path = "FEB_CAN.dbc"
db = cantools.database.load_file(dbc_path)

# Load the ASC log file
asc_path = "2025-07-29_17-13-13 -- 577 - Logging.asc"  # <-- Replace with your actual file
log = can.ASCReader(asc_path)
messages = list(log)

# List of decoded signal data
data = []
for msg in messages:
    try:
        message_def = db.get_message_by_frame_id(msg.arbitration_id)
        decoded = message_def.decode(msg.data)
        second = int(msg.timestamp)  # round down to full second
        for signal_name, value in decoded.items():
            data.append({"Second": second, "Signal": signal_name, "Value": value})
    except Exception:
        continue

# Convert to tidy DataFrame
df = pd.DataFrame(data)

# Pivot into wide format (seconds as rows, signals as columns)
df_pivot = (
    df.sort_values("Second")                      # sort chronologically
      .groupby(["Second", "Signal"])["Value"]
      .last()                                     # use the *last* value seen in that second
      .unstack("Signal")                          # make signals into columns
      .ffill()                                   
      .reset_index()                              # make "Second" a column again
)

df_pivot.to_csv("can_signals_by_second.csv", index=False)
print(df_pivot.head())