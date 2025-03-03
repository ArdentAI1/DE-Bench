import json
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os


def load_test_results(file_path="Results/Test_Results.json"):
    """Load test results from JSON file"""
    with open(file_path, "r") as f:
        return json.load(f)


def create_visualizations(results):
    """Create various visualizations from test results"""
    # Set style using seaborn
    sns.set_theme(style="whitegrid")
    sns.set_palette("husl")

    # Create a figure with multiple subplots
    fig = plt.figure(figsize=(15, 10))
    fig.suptitle(
        f'Test Results Analysis\nSession ID: {results["session_id"]}', fontsize=14
    )

    # 1. Model Runtime Bar Chart
    ax1 = plt.subplot(2, 2, 1)
    test_names = [
        result["nodeid"].split("::")[-1] for result in results["test_results"]
    ]
    model_runtimes = [result["model_runtime"] for result in results["test_results"]]

    sns.barplot(x=model_runtimes, y=test_names, ax=ax1)
    ax1.set_title("Model Execution Duration")
    ax1.set_xlabel("Duration (seconds)")
    ax1.set_ylabel("Test Name")

    # 2. Test Outcomes Pie Chart
    ax2 = plt.subplot(2, 2, 2)
    outcomes = [result["outcome"] for result in results["test_results"]]
    outcome_counts = {
        "passed": outcomes.count("passed"),
        "failed": outcomes.count("failed"),
        "skipped": outcomes.count("skipped"),
    }

    colors = {"passed": "green", "failed": "red", "skipped": "gray"}
    pie_colors = [
        colors[outcome]
        for outcome in outcome_counts.keys()
        if outcome_counts[outcome] > 0
    ]

    filtered_outcomes = {k: v for k, v in outcome_counts.items() if v > 0}
    ax2.pie(
        filtered_outcomes.values(),
        labels=[f"{k} ({v})" for k, v in filtered_outcomes.items()],
        colors=pie_colors,
        autopct="%1.1f%%",
    )
    ax2.set_title("Test Outcomes Distribution")

    # 3. Model Runtime Distribution
    ax3 = plt.subplot(2, 2, 3)
    sns.histplot(model_runtimes, ax=ax3, bins=10)
    ax3.set_title("Model Runtime Distribution")
    ax3.set_xlabel("Duration (seconds)")
    ax3.set_ylabel("Count")

    # 4. Summary Statistics
    ax4 = plt.subplot(2, 2, 4)
    ax4.axis("off")

    # Calculate both total and model runtimes
    total_durations = [result["duration"] for result in results["test_results"]]
    model_runtimes = [result["model_runtime"] for result in results["test_results"]]

    summary_text = (
        f"Summary Statistics\n\n"
        f"Total Tests: {len(results['test_results'])}\n"
        f"Total Test Duration: {sum(total_durations):.2f}s\n"
        f"Total Model Runtime: {sum(model_runtimes):.2f}s\n"
        f"Average Model Runtime: {sum(model_runtimes)/len(model_runtimes):.2f}s\n"
        f"Max Model Runtime: {max(model_runtimes):.2f}s\n"
        f"Min Model Runtime: {min(model_runtimes):.2f}s\n"
        f"\nTest Outcomes:\n"
        f"Passed: {outcome_counts['passed']}\n"
        f"Failed: {outcome_counts['failed']}\n"
        f"Skipped: {outcome_counts['skipped']}"
    )
    ax4.text(0.1, 0.9, summary_text, fontsize=10, va="top")

    # Adjust layout and save
    plt.tight_layout()

    # Create results directory if it doesn't exist
    os.makedirs("Results/visualizations", exist_ok=True)

    # Save with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    plt.savefig(
        f"Results/visualizations/test_results_{timestamp}.png",
        dpi=300,
        bbox_inches="tight",
    )

    # Optionally display the plot
    plt.show()


if __name__ == "__main__":
    results = load_test_results()
    create_visualizations(results)
