# python -m scripts.visualize_replay --match-id 832ceecc424df110d58e3e96d3dff834 --hz 20 --stride 5 --max-frames 2000
import argparse

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation

from etl.parsing.replay_parsing import get_match_object


def build_colors(num_players: int):
    cmap = plt.get_cmap("tab20")
    colors = [cmap(i % 20) for i in range(num_players)]
    return np.array(colors)


def compute_bounds(frames, sample_stride: int = 50):
    if not frames:
        return (-1, 1, -1, 1)
    sample = frames[::sample_stride]
    xs = np.concatenate([f[:, 0] for f in sample])
    ys = np.concatenate([f[:, 1] for f in sample])
    mask = np.isfinite(xs) & np.isfinite(ys)
    if not np.any(mask):
        return (-1, 1, -1, 1)
    xs = xs[mask]
    ys = ys[mask]
    min_x, max_x = float(xs.min()), float(xs.max())
    min_y, max_y = float(ys.min()), float(ys.max())
    pad_x = (max_x - min_x) * 0.05 or 1.0
    pad_y = (max_y - min_y) * 0.05 or 1.0
    return (min_x - pad_x, max_x + pad_x, min_y - pad_y, max_y + pad_y)


def summarize_frames(frames, alive_only: bool = False):
    if not frames:
        return "No frames."
    sample = frames[:: max(1, len(frames) // 10)]
    xs = np.concatenate([f[:, 0] for f in sample])
    ys = np.concatenate([f[:, 1] for f in sample])
    finite = np.isfinite(xs) & np.isfinite(ys)
    alive = np.concatenate([f[:, 6] for f in sample]) > 0.5
    if alive_only:
        finite = finite & alive
    count = int(np.sum(finite))
    total = int(xs.size)
    if count == 0:
        return f"Finite positions: 0/{total}"
    xs = xs[finite]
    ys = ys[finite]
    return (
        f"Finite positions: {count}/{total} | "
        f"x=[{xs.min():.1f}, {xs.max():.1f}] "
        f"y=[{ys.min():.1f}, {ys.max():.1f}]"
    )


def animate_frames(
    frames,
    hz: int,
    stride: int = 1,
    alive_only: bool = False,
    interp: int = 1,
    save_gif: str | None = None,
    save_static: str | None = None,
):
    if not frames:
        raise ValueError("No frames to animate.")

    interp = max(1, int(interp))
    base_dt = stride / hz
    dt = base_dt / interp
    num_players = frames[0].shape[0]
    colors = build_colors(num_players)

    fig, ax = plt.subplots(figsize=(8, 8))
    fig.patch.set_facecolor("black")
    ax.set_facecolor("black")
    ax.tick_params(colors="white")

    bounds = compute_bounds(frames)
    ax.set_xlim(bounds[0], bounds[1])
    ax.set_ylim(bounds[2], bounds[3])

    scatter = ax.scatter([], [], s=10)
    label = ax.text(
        0.02,
        0.98,
        "",
        transform=ax.transAxes,
        color="white",
        va="top",
        ha="left",
    )

    def get_mask(frame, alive_mask=None):
        mask = np.isfinite(frame[:, 0]) & np.isfinite(frame[:, 1])
        if alive_only:
            if alive_mask is None:
                alive_mask = frame[:, 6] > 0.5
            mask = mask & alive_mask
        return mask

    total_steps = (len(frames) - 1) * interp + 1

    def update(step_idx):
        base_idx = min(step_idx // interp, len(frames) - 1)
        next_idx = min(base_idx + 1, len(frames) - 1)
        alpha = (step_idx % interp) / interp if next_idx != base_idx else 0.0

        f0 = frames[base_idx]
        f1 = frames[next_idx]
        frame = f0 + alpha * (f1 - f0)

        alive_mask = np.maximum(f0[:, 6], f1[:, 6]) > 0.5
        mask = get_mask(frame, alive_mask=alive_mask)
        xs = frame[:, 0][mask]
        ys = frame[:, 1][mask]
        scatter.set_offsets(np.column_stack((xs, ys)))
        scatter.set_facecolor(colors[mask].tolist())
        label.set_text(f"step {step_idx + 1}/{total_steps}  t={step_idx * dt:.2f}s")
        return scatter, label

    anim = animation.FuncAnimation(
        fig,
        update,
        frames=total_steps,
        interval=dt * 1000,
        blit=True,
    )
    if save_static:
        update(0)
        fig.savefig(save_static, dpi=160, facecolor=fig.get_facecolor())
    if save_gif:
        fps = max(1, int((hz / stride) * interp))
        anim.save(save_gif, writer=animation.PillowWriter(fps=fps))
    if not save_gif:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Visualize replay frames as moving dots."
    )
    parser.add_argument("--match-id", required=True)
    parser.add_argument("--hz", type=int, default=20)
    parser.add_argument("--stride", type=int, default=5)
    parser.add_argument("--max-frames", type=int, default=None)
    parser.add_argument("--alive-only", action="store_true")
    parser.add_argument("--interp", type=int, default=1)
    parser.add_argument("--save-gif", default=None)
    parser.add_argument("--save-static", default=None)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    result = get_match_object(match_id=args.match_id, hz=args.hz)
    if isinstance(result, tuple) and len(result) == 2:
        player_index, frames = result
    else:
        player_index, frames = None, result
    if args.stride > 1:
        frames = frames[:: args.stride]
    if args.max_frames:
        frames = frames[: args.max_frames]

    if args.debug:
        if player_index is not None:
            print(f"Players: {len(player_index)}")
        print(f"Frames: {len(frames)}")
        print(summarize_frames(frames, alive_only=args.alive_only))

    if args.save_gif or args.save_static:
        plt.switch_backend("Agg")

    animate_frames(
        frames,
        hz=args.hz,
        stride=max(1, args.stride),
        alive_only=args.alive_only,
        interp=max(1, args.interp),
        save_gif=args.save_gif,
        save_static=args.save_static,
    )


if __name__ == "__main__":
    main()
