"""Figures for the report: architecture diagram, window results, load test results."""
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

HERE = Path(__file__).parent
OUT, FIG = HERE / "outputs", HERE / "figures"
FIG.mkdir(exist_ok=True)


def box(ax, x, y, w, h, text, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                                fc=color, ec="#333333", lw=1.2))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=8.5)


def arrow(ax, x1, y1, x2, y2, text=None):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=12,
                                 lw=1.1, color="#333333"))
    if text:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.12, text, ha="center", fontsize=7, color="#444444")


def architecture():
    fig, ax = plt.subplots(figsize=(11, 5.4))
    ax.set_xlim(0, 11)
    ax.set_ylim(0.3, 5.6)
    ax.axis("off")

    # producers, the partition of each key is taken from the producer delivery logs
    mapping = {"G01": 2, "G02": 1, "G03": 0}
    part_y = {0: 3.55, 1: 2.7, 2: 1.85}
    for i, g in enumerate(["G01", "G02", "G03"]):
        y = 3.9 - i * 1.25
        box(ax, 0.2, y, 1.8, 0.8, f"Producer {g}\nproducer.py --group {g}\nkey = {g}", "#dbe9f6")

    ax.add_patch(FancyBboxPatch((2.7, 0.55), 2.9, 4.55, boxstyle="round,pad=0.02,rounding_size=0.1",
                                fc="#fff4dc", ec="#b07d10", lw=1.4))
    ax.text(4.15, 4.85, "Kafka broker 4.3.2 (KRaft, localhost:9092)", ha="center", fontsize=8.5, weight="bold")
    ax.text(4.15, 4.42, "topic aitu.research.jobs\n3 partitions, RF = 1, retention 7 days", ha="center",
            fontsize=8, va="center")
    for pnum, y in part_y.items():
        box(ax, 3.0, y, 2.3, 0.6, f"partition {pnum}", "#ffe2a8")
    for i, g in enumerate(["G01", "G02", "G03"]):
        arrow(ax, 2.0, 4.3 - i * 1.25, 3.0, part_y[mapping[g]] + 0.3)
    box(ax, 3.0, 0.75, 2.3, 0.65, "DLQ topic\naitu.research.jobs.dlq", "#f6d5d5")

    ax.add_patch(FancyBboxPatch((6.1, 2.95), 2.4, 2.15, boxstyle="round,pad=0.02,rounding_size=0.1",
                                fc="#e6f4e6", ec="#3d7a3d", lw=1.4))
    ax.text(7.3, 4.85, "consumer group aitu-analytics", ha="center", fontsize=8, weight="bold")
    box(ax, 6.3, 3.1, 2.0, 1.5, "Python processor\nconsumer.py\nschema check, counts,\n5 s event-time windows",
        "#c9eac9")
    box(ax, 6.2, 1.15, 2.2, 1.1, "Spark Structured\nStreaming job\n(own consumer group)", "#c9eac9")
    arrow(ax, 5.6, 3.85, 6.3, 3.85, "poll")
    arrow(ax, 5.6, 1.7, 6.2, 1.7, "readStream")
    ax.add_patch(FancyArrowPatch((6.3, 3.2), (5.3, 1.05), arrowstyle="-|>", mutation_scale=12, lw=1.1,
                                 color="#a33", connectionstyle="arc3,rad=0.35"))
    ax.text(5.75, 2.45, "invalid", fontsize=7, color="#a33")

    box(ax, 9.0, 3.6, 1.8, 1.0, "Aggregation layer\njob / group stats,\nwindow results\n(outputs/*.csv)", "#e8e0f4")
    box(ax, 9.0, 2.2, 1.8, 1.0, "Monitoring\nlag, throughput,\nlatency, CPU,\nfailure rate", "#f4e0ea")
    box(ax, 9.0, 0.8, 1.8, 1.0, "Dashboard / alert\nfailure rate > 30 %\nor lag growing", "#eeeeee")
    arrow(ax, 8.3, 4.0, 9.0, 4.1)
    arrow(ax, 8.4, 1.7, 9.0, 3.75)
    arrow(ax, 9.9, 3.6, 9.9, 3.2)
    arrow(ax, 9.9, 2.2, 9.9, 1.8)

    ax.set_title("AITU research event pipeline (Assignment 4)", fontsize=11)
    fig.tight_layout()
    fig.savefig(FIG / "partA_architecture.png", dpi=170)
    plt.close(fig)


def windows():
    w = pd.read_csv(OUT / "partC_windows.csv")
    fig, ax1 = plt.subplots(figsize=(7, 3.7))
    x = range(len(w))
    ax1.bar(x, w["count"], color="#7aa6d6", label="events")
    ax1.set_ylabel("events in window")
    ax1.set_xticks(list(x), w["window"])
    ax2 = ax1.twinx()
    ax2.plot(x, w["avg_cpu"], "o-", color="#c0392b", label="average CPU %")
    ax2.plot(x, w["failure_rate_pct"], "s--", color="#555555", label="failure rate %")
    ax2.set_ylim(0, 100)
    ax2.set_ylabel("percent")
    for i, r in w.iterrows():
        ax2.annotate(f"{r['avg_cpu']:.2f}", (i, r["avg_cpu"]), textcoords="offset points",
                     xytext=(0, 6), ha="center", fontsize=8, color="#c0392b")
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    fig.legend(h1 + h2, l1 + l2, fontsize=8, loc="lower center", ncol=3, frameon=False)
    ax1.set_title("5-second event-time windows (consumer.py output)", fontsize=10)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(FIG / "partC_windows.png", dpi=170)
    plt.close(fig)


def load():
    df = pd.read_csv(OUT / "partE_load_results.csv")
    df = df[(df["commit"] == "batch") & (df["input_rate"] != "max")]
    df = df.assign(rate=df["input_rate"].astype(int)).sort_values("rate")
    base = df[df["batch_timeout_s"] == 1.0]
    opt = df[df["batch_timeout_s"] == 0.05]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 3.4))
    a1.plot(base["rate"], base["rate"], "k:", label="ideal (= input rate)")
    a1.plot(base["rate"], base["observed_throughput"], "o-", label="observed")
    a1.set_ylabel("throughput (events/s)")
    a1.set_title("Throughput (baseline settings)", fontsize=10)
    a2.plot(base["rate"], base["avg_latency_ms"], "o-", label="baseline, batch timeout 1 s")
    a2.plot(opt["rate"], opt["avg_latency_ms"], "s-", color="#2a9d4b", label="optimized, batch timeout 0.05 s")
    a2.set_ylabel("average latency (ms)")
    a2.set_title("End-to-end latency", fontsize=10)
    for a in (a1, a2):
        a.set_xscale("log")
        a.set_xticks([10, 100, 500], ["10", "100", "500"])
        a.set_xlabel("input rate (events/s)")
        a.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "partE_load.png", dpi=170)
    plt.close(fig)


if __name__ == "__main__":
    architecture()
    for f in (windows, load):
        try:
            f()
        except FileNotFoundError as e:
            print("skipped", f.__name__, e)
