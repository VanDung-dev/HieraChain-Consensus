"""
Benchmark analysis script for comparing Rust and Python implementations of OrderingService.

This script reads benchmark results from a JSON file and generates a performance
comparison chart.
"""

import os
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
    plt.title('Python vs Rust Performance Comparison')
    plt.xlabel('Number of events')
    plt.ylabel('Events/sec')
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
    plt.xlabel('Number of Events')
    plt.ylabel('Block retrieval time(s)')
    plt.legend()
    plt.grid()

    plt.tight_layout()
    
    # Save chart to same directory as input file if possible, or relative output dir
    output_dir = os.path.dirname(file_path)
    if not output_dir:
        output_dir = '.'
    chart_path = os.path.join(output_dir, 'OrderingService_benchmark.png')
    plt.savefig(chart_path)
    print(f"Chart saved to '{chart_path}'")

if __name__ == '__main__':
    # Determine project root relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    results_path = os.path.join(project_root, 'output', 'OrderingService_benchmark.json')
    
    print(f"DEBUG: Reading results from: {results_path}")
    analyze_benchmark(results_path)
