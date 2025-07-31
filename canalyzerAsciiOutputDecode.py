import can
import cantools.database as candb
import polars as pl
import matplotlib.pyplot as plt


# Load the DBC file
dbc_path = "FEBStuff/FEB_CAN.dbc"
inv_dbc_path = "FEBStuff/20240815 PM and RM CAN DB.dbc"
db = candb.load_file(dbc_path)
inv_db = candb.load_file(inv_dbc_path)


# Load the ASC log file
asc_path = "FEBStuff/2025-07-29_17-13-13 -- 577 - Logging.asc"  # <-- Replace with your actual file
log = can.ASCReader(asc_path)
messages = list(log)

# List of decoded signal data
data = []
for msg in messages:
    try:
        message_def = db.get_message_by_frame_id(msg.arbitration_id)
        decoded = message_def.decode(msg.data)
        centisecond = int(msg.timestamp*10)  # round down to full second
        for signal_name, value in decoded.items():
            if signal_name[0:5] != "error":
                data.append({"Second": centisecond/10, "Signal": signal_name, "Value": value})
    except Exception:
        continue

invdata = []
for msg in messages:
    try:
        message_def = inv_db.get_message_by_frame_id(msg.arbitration_id)
        decoded = message_def.decode(msg.data)
        centisecond = int(msg.timestamp*10)  # round down to full second
        for signal_name, value in decoded.items():
            if signal_name[0:5] != "error":
                invdata.append({"Second": centisecond/10, "Signal": signal_name, "Value": value})
    except Exception:
        continue

# Convert to tidy DataFrame

# Ensure all "Value" entries are floats for polars compatibility
for entry in data:
    try:
        entry["Value"] = float(entry["Value"])
    except (ValueError, TypeError):
        entry["Value"] = None

for entry in invdata:
    try:
        entry["Value"] = float(entry["Value"])
    except (ValueError, TypeError):
        entry["Value"] = None

df = pl.DataFrame(data)
invdf = pl.DataFrame(invdata)

# Pivot into wide format (seconds as rows, signals as columns)
df = (
    df.sort("Second")
      .group_by(["Second", "Signal"])
      .agg(pl.col("Value").last())
      .pivot(values="Value", index="Second", on="Signal")
      .sort("Second")
      .fill_null(strategy="forward")
)

invdf = (
    invdf.sort("Second")
      .group_by(["Second", "Signal"])
      .agg(pl.col("Value").last())
      .pivot(values="Value", index="Second", on="Signal")
      .sort("Second")
      .fill_null(strategy="forward")
)
df.columns
invdf.columns

plt.plot(df["Second"],df["brake2_psi"])
plt.plot(df["Second"],df["acc0"])
plt.plot(df["Second"],df["torque_signal_small"])
plt.plot(invdf["Second"],invdf["INV_Motor_Speed"])
plt.legend(["brake2", "acc0", "torque", "speed"])
plt.show()
