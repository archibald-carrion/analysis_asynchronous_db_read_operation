#!/usr/bin/env python3
"""
TPC-H Power Test vs Throughput Test Diagram Generator
Dependencies: matplotlib, numpy
"""

import sys
import os

try:
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.patches import Rectangle, FancyBboxPatch, Circle
    from matplotlib.patches import ArrowStyle
except ImportError as e:
    print(f"Error: Missing dependency - {e}")
    print("Please install dependencies using:")
    print("  pip install matplotlib numpy")
    sys.exit(1)

def create_tpch_tests_diagram():
    """Create TPC-H Power Test vs Throughput Test explanation diagram"""
    
    # Set style for academic paper
    plt.style.use('default')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), dpi=300)

    # Colors for consistent styling
    refresh_color = '#9b59b6'
    query_color = '#2ecc71'
    power_test_color = '#3498db'
    throughput_test_color = '#e74c3c'
    sequential_color = '#2980b9'
    parallel_color = '#c0392b'
    text_color = '#2c3e50'

    def draw_power_test(ax):
        """Draw Power Test explanation"""
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 1)
        ax.set_title("TPC-H Power Test: Single-Stream Performance", fontsize=14, fontweight='bold', pad=20, color=text_color)
        
        # Main timeline
        ax.axhline(y=0.5, color='#7f8c8d', linewidth=2, alpha=0.7)
        
        # Power Test blocks
        power_blocks = [
            {'type': 'refresh', 'start': 5, 'duration': 8, 'y': 0.5, 'label': 'RF1\nRefresh'},
            {'type': 'query', 'start': 18, 'duration': 60, 'y': 0.5, 'label': 'Q1-Q22\nSequential'},
            {'type': 'refresh', 'start': 83, 'duration': 8, 'y': 0.5, 'label': 'RF2\nRefresh'},
        ]
        
        for block in power_blocks:
            if block['type'] == 'refresh':
                color = refresh_color
                edgecolor = '#8e44ad'
            else:
                color = power_test_color
                edgecolor = '#2980b9'
            
            rect = Rectangle((block['start'], block['y']-0.15), 
                            block['duration'], 0.3,
                            facecolor=color, edgecolor=edgecolor, linewidth=1.5,
                            alpha=0.9)
            ax.add_patch(rect)
            
            # Add label
            ax.text(block['start'] + block['duration']/2, block['y'], 
                    block['label'], ha='center', va='center', 
                    fontweight='bold', color='white', fontsize=9)

        # Arrows showing sequence
        arrows = [
            {'x': 13, 'y': 0.5, 'dx': 5, 'dy': 0, 'color': 'black'},
            {'x': 78, 'y': 0.5, 'dx': 5, 'dy': 0, 'color': 'black'},
        ]
        
        for arrow in arrows:
            ax.arrow(arrow['x'], arrow['y'], arrow['dx'], arrow['dy'],
                     head_width=0.03, head_length=1.5, fc=arrow['color'], ec=arrow['color'],
                     linewidth=2)

        # Add explanatory annotations
        ax.text(50, 0.85, "SINGLE STREAM EXECUTION", ha='center', va='center', 
                fontweight='bold', fontsize=12, color=sequential_color,
                bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', edgecolor=sequential_color))
        
        ax.text(50, 0.2, "Measures:\n• Raw query processing power\n• Single-user performance\n• Optimized execution time", 
                ha='center', va='center', fontsize=10, color=text_color,
                bbox=dict(boxstyle="round,pad=0.5", facecolor='lightyellow', edgecolor='orange', alpha=0.8))

        # Query execution details
        ax.text(18, 0.7, "Sequential Query Execution:", fontsize=9, fontweight='bold', color=text_color)
        ax.text(18, 0.65, "Q1 → Q2 → Q3 → ... → Q22", fontsize=8, color=text_color, style='italic')
        
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)

    def draw_throughput_test(ax):
        """Draw Throughput Test explanation"""
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 1)
        ax.set_title("TPC-H Throughput Test: Multi-Stream Concurrent Performance", fontsize=14, fontweight='bold', pad=20, color=text_color)
        
        # Multiple parallel timelines
        streams = [0.7, 0.5, 0.3]  # Three query streams
        refresh_stream = 0.1        # Refresh stream
        
        for i, y in enumerate(streams):
            ax.axhline(y=y, color='#7f8c8d', linewidth=1.5, alpha=0.7, linestyle='-')
            ax.text(-5, y, f"Stream {i+1}", ha='right', va='center', fontweight='bold', fontsize=9, color=text_color)
        
        # Refresh stream
        ax.axhline(y=refresh_stream, color=refresh_color, linewidth=2, alpha=0.7, linestyle='--')
        ax.text(-5, refresh_stream, "Refresh\nStream", ha='right', va='center', fontweight='bold', fontsize=9, color=refresh_color)

        # Query blocks for each stream (random order simulation)
        stream_queries = [
            [3, 7, 1, 5, 2, 6, 4],  # Stream 1
            [2, 4, 6, 1, 3, 5, 7],  # Stream 2  
            [5, 1, 3, 6, 2, 4, 7],  # Stream 3
        ]
        
        # Draw query blocks
        for stream_idx, queries in enumerate(stream_queries):
            y_pos = streams[stream_idx]
            x_pos = 10
            for query_num in queries:
                duration = 8 + (query_num % 3)  # Vary duration slightly
                
                rect = Rectangle((x_pos, y_pos-0.08), 
                                duration, 0.16,
                                facecolor=query_color, edgecolor='#27ae60', linewidth=1.2,
                                alpha=0.8)
                ax.add_patch(rect)
                
                ax.text(x_pos + duration/2, y_pos, f"Q{query_num}", 
                        ha='center', va='center', fontweight='bold', color='white', fontsize=7)
                
                x_pos += duration + 2

        # Refresh function blocks
        refresh_pairs = [
            {'start': 15, 'duration': 6, 'label': 'RF1'},
            {'start': 45, 'duration': 6, 'label': 'RF2'},
            {'start': 75, 'duration': 6, 'label': 'RF1'},
        ]
        
        for refresh in refresh_pairs:
            rect = Rectangle((refresh['start'], refresh_stream-0.06), 
                            refresh['duration'], 0.12,
                            facecolor=refresh_color, edgecolor='#8e44ad', linewidth=1.5,
                            alpha=0.9)
            ax.add_patch(rect)
            
            ax.text(refresh['start'] + refresh['duration']/2, refresh_stream, 
                    refresh['label'], ha='center', va='center', 
                    fontweight='bold', color='white', fontsize=8)

        # Add explanatory annotations
        ax.text(50, 0.9, "PARALLEL EXECUTION", ha='center', va='center', 
                fontweight='bold', fontsize=12, color=parallel_color,
                bbox=dict(boxstyle="round,pad=0.5", facecolor='lightcoral', edgecolor=parallel_color))
        
        ax.text(50, 0.2, "Measures:\n• Concurrent query processing\n• System throughput under load\n• Resource contention handling", 
                ha='center', va='center', fontsize=10, color=text_color,
                bbox=dict(boxstyle="round,pad=0.5", facecolor='lightyellow', edgecolor='orange', alpha=0.8))

        # Concurrent execution indicators
        ax.text(80, 0.65, "Random Query Order\nPer Stream", fontsize=8, color=text_color, 
                ha='center', va='center', style='italic',
                bbox=dict(boxstyle="round,pad=0.3", facecolor='white', edgecolor='gray', alpha=0.7))

        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)

    # Draw both tests
    draw_power_test(ax1)
    draw_throughput_test(ax2)

    # Add overall legend
    legend_elements = [
        Rectangle((0, 0), 1, 1, facecolor=power_test_color, edgecolor='#2980b9', label='Power Test Queries'),
        Rectangle((0, 0), 1, 1, facecolor=query_color, edgecolor='#27ae60', label='Throughput Test Queries'),
        Rectangle((0, 0), 1, 1, facecolor=refresh_color, edgecolor='#8e44ad', label='Refresh Functions'),
    ]

    fig.legend(handles=legend_elements, 
               loc='lower center', 
               bbox_to_anchor=(0.5, 0.02),
               ncol=3, 
               frameon=True,
               fontsize=11,
               fancybox=True,
               shadow=False,
               framealpha=1)

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.1, hspace=0.3)  # Make room for legend

    # Save as high-quality PNG
    output_file = 'tpch_power_vs_throughput.png'
    plt.savefig(output_file, 
                dpi=300, 
                bbox_inches='tight', 
                facecolor='white',
                edgecolor='none')

    plt.close()
    
    return output_file

def create_tpch_metrics_diagram():
    """Create TPC-H metrics calculation explanation diagram"""
    
    fig, ax = plt.subplots(1, 1, figsize=(12, 6), dpi=300)
    
    text_color = '#2c3e50'
    
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 1)
    ax.set_title("TPC-H Metrics Calculation", fontsize=16, fontweight='bold', pad=20, color=text_color)
    
    # Power Metric Formula
    power_box = FancyBboxPatch((5, 0.6), 90, 0.35, boxstyle="round,pad=0.1", 
                              facecolor='lightblue', edgecolor='#3498db', linewidth=2)
    ax.add_patch(power_box)
    
    ax.text(50, 0.8, "POWER@Size = 3600 × SF × √[1 / (∏ QI(i,0) × ∏ RI(j,0))]^(1/24)", 
            ha='center', va='center', fontsize=12, fontweight='bold', color=text_color)
    
    ax.text(50, 0.7, "Where: QI(i,0) = Query times from Power Test, RI(j,0) = Refresh times from Power Test", 
            ha='center', va='center', fontsize=10, color=text_color)
    
    # Throughput Metric Formula
    throughput_box = FancyBboxPatch((5, 0.2), 90, 0.35, boxstyle="round,pad=0.1", 
                                   facecolor='lightcoral', edgecolor='#e74c3c', linewidth=2)
    ax.add_patch(throughput_box)
    
    ax.text(50, 0.4, "THROUGHPUT@Size = (S × 22 × 3600) / T_s", 
            ha='center', va='center', fontsize=12, fontweight='bold', color=text_color)
    
    ax.text(50, 0.3, "Where: S = Query streams, T_s = Measurement interval (Ts)", 
            ha='center', va='center', fontsize=10, color=text_color)
    
    # Final Metric
    final_box = FancyBboxPatch((30, 0.05), 40, 0.1, boxstyle="round,pad=0.1", 
                              facecolor='lightgreen', edgecolor='#2ecc71', linewidth=2)
    ax.add_patch(final_box)
    
    ax.text(50, 0.1, "QphH@Size = 1 / sqrt((1 / Power@Size) × (1 / Throughput@Size))", 
            ha='center', va='center', fontsize=12, fontweight='bold', color=text_color)
    
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)

    output_file = 'tpch_metrics_calculation.png'
    plt.savefig(output_file, 
                dpi=300, 
                bbox_inches='tight', 
                facecolor='white',
                edgecolor='none')
    
    plt.close()
    
    return output_file

if __name__ == "__main__":
    try:
        output_file1 = create_tpch_tests_diagram()
        output_file2 = create_tpch_metrics_diagram()
        print(f"✅ TPC-H tests explanation diagram saved as '{output_file1}'")
        print(f"✅ TPC-H metrics calculation diagram saved as '{output_file2}'")
    except Exception as e:
        print(f"❌ Error creating diagram: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)