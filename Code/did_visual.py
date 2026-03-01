import numpy as np
import matplotlib.pyplot as plt

# Time grids
t_pre = np.linspace(0, 5, 200)
t_post = np.linspace(5, 10, 200)
t_full = np.linspace(0, 10, 400)

# === Control (blue) ===
control = 0.25 * t_full + 2
control_cf = np.full_like(t_full, 2)

# === Treatment (red) ===
treat_pre = 0.25 * t_pre + 4
treat_cf_post = 0.25 * t_post + 4          # dashed continuation
treat_post = 0.50 * t_post + 4 - 1.25       # steeper observed post (matches your Desmos)

# Plot
fig, ax = plt.subplots(figsize=(8, 5))

# Treatment
ax.plot(t_pre, treat_pre, color="red", linewidth=2, label="Treatment")
ax.plot(t_post, treat_cf_post, color="red", linestyle="--", linewidth=2)
ax.plot(t_post, treat_post, color="red", linewidth=2)

# Control
ax.plot(t_full, control, color="blue", linewidth=2, label="Control")
ax.plot(t_full, control_cf, color="blue", linestyle="--", linewidth=2)

# PNTR line
ax.axvline(5, color="black", linestyle="--")
ax.text(5, ax.get_ylim()[1]*0.96, "PNTR", ha="center", va="top")

# === PERFECTLY VERTICAL GAP INDICATORS ===

# δ_p : intercept gap
ax.vlines(x=-0.1, ymin=2, ymax=4, color="black", linewidth=1.5)
ax.text(0, 3, r"$\delta_p$", va="center")

# λ_t : time fixed effect (control)
y_control_10 = control[-1]
ax.vlines(x=10.1, ymin=2, ymax=y_control_10, color="black", linewidth=1.5)
ax.text(10.2, (2 + y_control_10)/2, r"$\lambda_t$", va="center")

# β : treatment effect
y_cf_10 = treat_cf_post[-1]
y_obs_10 = treat_post[-1]
ax.vlines(x=10.1, ymin=y_cf_10, ymax=y_obs_10, color="black", linewidth=1.5)
ax.text(10.2, (y_cf_10 + y_obs_10)/2, r"$\beta$", va="center")

# Labels
ax.set_xlabel("time")
ax.set_ylabel(r"$Y_{pt}$")

# Keep axes, remove numbers
ax.set_xticks([])
ax.set_yticks([])
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

ax.legend(frameon=False)
plt.tight_layout()
plt.savefig("did_visual.pdf", format="pdf", bbox_inches="tight")
plt.show()

