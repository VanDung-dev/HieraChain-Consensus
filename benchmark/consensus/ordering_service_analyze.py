import json
import matplotlib.pyplot as plt

def analyze_benchmark(file_path):
    # Read data from a JSON file
    with open(file_path) as f:
        data = json.load(f)

    # Split data by language
    python_data = [d for d in data if d['implementation'] == 'Python']
    rust_data = [d for d in data if d['implementation'] == 'Rust']

    # Draw a performance comparison chart
    plt.figure(figsize=(10, 6))

    # events_per_second chart
    plt.subplot(2, 1, 1)
    plt.plot([d['event_count'] for d in python_data],
             [d['events_per_second_submission'] for d in python_data],
             label='Python')
    plt.plot([d['event_count'] for d in rust_data],
             [d['events_per_second_submission'] for d in rust_data],
             label='Rust')
    plt.title('So sánh hiệu suất Python vs Rust')
    plt.xlabel('Số lượng sự kiện')
    plt.ylabel('Sự kiện/giây')
    plt.legend()
    plt.grid()

    # Block retrieval time chart
    plt.subplot(2, 1, 2)
    plt.plot([d['event_count'] for d in python_data],
             [d['block_retrieval_time'] for d in python_data],
             label='Python')
    plt.plot([d['event_count'] for d in rust_data],
             [d['block_retrieval_time'] for d in rust_data],
             label='Rust')
    plt.xlabel('Số lượng sự kiện')
    plt.ylabel('Thời gian truy xuất block (s)')
    plt.legend()
    plt.grid()

    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    analyze_benchmark('../benchmark_results.json')
