#!/usr/bin/env python3
"""
TPC-H Benchmark Execution Diagram Generator
Dependencies: matplotlib, numpy
"""

import sys
import os

try:
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.patches import Rectangle
except ImportError as e:
    print(f"Error: Missing dependency - {e}")
    print("Please install dependencies using:")
    print("  pip install matplotlib numpy")
    sys.exit(1)

def create_tpch_benchmark_diagram():
    """Create TPC-H benchmark execution timeline diagram"""
    
    # Set style for academic paper
    plt.style.use('default')
    fig, ax = plt.subplots(1, 1, figsize=(14, 8), dpi=300)

    # Colors for consistent styling
    power_test_color = '#3498db'
    throughput_test_color = '#e74c3c'
    refresh_color = '#9b59b6'
    query_color = '#2ecc71'
    setup_color = '#f39c12'
    text_color = '#2c3e50'

    def draw_timeline(ax, title, blocks, y_positions, test_types):
        """Draw a timeline showing benchmark execution process"""
        ax.set_xlim(0, 180)
        ax.set_ylim(0, 1)
        ax.set_title(title, fontsize=16, fontweight='bold', pad=20, color=text_color)
        
        # Draw timelines for each test type
        for i, y_pos in enumerate(y_positions):
            ax.axhline(y=y_pos, color='#7f8c8d', linewidth=1.5, alpha=0.7)
            ax.text(-25, y_pos, "Test Type:", ha='right', va='center', 
                    fontweight='bold', fontsize=11, color=text_color)
        
        # Draw test type labels on the right
        for i, test_type in enumerate(test_types):
            ax.text(185, y_positions[i], test_type, ha='left', va='center', 
                    fontweight='bold', fontsize=10, color=text_color,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor='lightgray', alpha=0.7))
        
        # Draw blocks
        for block in blocks:
            if block['type'] == 'setup':
                color = setup_color
                edgecolor = '#e67e22'
            elif block['type'] == 'power_test':
                color = power_test_color
                edgecolor = '#2980b9'
            elif block['type'] == 'throughput_test':
                color = throughput_test_color
                edgecolor = '#c0392b'
            elif block['type'] == 'refresh':
                color = refresh_color
                edgecolor = '#8e44ad'
            elif block['type'] == 'query':
                color = query_color
                edgecolor = '#27ae60'
            else:
                color = '#bdc3c7'
                edgecolor = '#95a5a6'
            
            rect = Rectangle((block['start'], block['y']-0.08), 
                            block['duration'], 0.16,
                            facecolor=color, edgecolor=edgecolor, linewidth=1.2,
                            alpha=0.9)
            ax.add_patch(rect)
            
            # Add label inside the block for better visibility
            label_y = block['y']
            ax.text(block['start'] + block['duration']/2, label_y, 
                    block['label'], ha='center', va='center', 
                    fontweight='bold', color='white', fontsize=8)
        
        # Add phase labels at the top
        phases = [
            {'name': 'Setup', 'start': 0, 'end': 10},
            {'name': 'Power Test', 'start': 10, 'end': 80},
            {'name': 'Throughput Test', 'start': 80, 'end': 180}
        ]
        
        for phase in phases:
            ax.axvspan(phase['start'], phase['end'], alpha=0.1, color='gray')
            ax.text((phase['start'] + phase['end'])/2, 0.95, phase['name'], 
                    ha='center', va='center', fontweight='bold', fontsize=12,
                    bbox=dict(boxstyle="round,pad=0.4", facecolor='white', edgecolor='gray'))

        # Remove axes
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)

    # Benchmark execution for different I/O methods
    test_types = ['Sync I/O', 'BG Workers', 'io_uring']
    y_positions = [0.7, 0.4, 0.1]

    # Blocks for different I/O methods
    blocks = [
        # Sync I/O
        {'type': 'setup', 'start': 2, 'duration': 6, 'y': 0.7, 'label': 'PostgreSQL\nConfig'},
        {'type': 'refresh', 'start': 12, 'duration': 8, 'y': 0.7, 'label': 'RF1'},
        {'type': 'power_test', 'start': 25, 'duration': 40, 'y': 0.7, 'label': 'Power Test\nQ1-Q22'},
        {'type': 'refresh', 'start': 70, 'duration': 8, 'y': 0.7, 'label': 'RF2'},
        {'type': 'throughput_test', 'start': 85, 'duration': 90, 'y': 0.7, 'label': 'Throughput\nTest'},
        
        # BG Workers (slightly faster)
        {'type': 'setup', 'start': 2, 'duration': 6, 'y': 0.4, 'label': 'PostgreSQL\nConfig'},
        {'type': 'refresh', 'start': 12, 'duration': 7, 'y': 0.4, 'label': 'RF1'},
        {'type': 'power_test', 'start': 24, 'duration': 38, 'y': 0.4, 'label': 'Power Test\nQ1-Q22'},
        {'type': 'refresh', 'start': 67, 'duration': 7, 'y': 0.4, 'label': 'RF2'},
        {'type': 'throughput_test', 'start': 82, 'duration': 85, 'y': 0.4, 'label': 'Throughput\nTest'},
        
        # io_uring (fastest)
        {'type': 'setup', 'start': 2, 'duration': 6, 'y': 0.1, 'label': 'PostgreSQL\nConfig'},
        {'type': 'refresh', 'start': 12, 'duration': 6, 'y': 0.1, 'label': 'RF1'},
        {'type': 'power_test', 'start': 23, 'duration': 36, 'y': 0.1, 'label': 'Power Test\nQ1-Q22'},
        {'type': 'refresh', 'start': 64, 'duration': 6, 'y': 0.1, 'label': 'RF2'},
        {'type': 'throughput_test', 'start': 78, 'duration': 80, 'y': 0.1, 'label': 'Throughput\nTest'},
    ]

    # Add arrows showing progression between phases for ALL I/O methods
    arrow_elements = [
        # Sync I/O arrows
        {'type': 'arrow', 'x': 8, 'y': 0.7, 'dx': 4, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 20, 'y': 0.7, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 65, 'y': 0.7, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 78, 'y': 0.7, 'dx': 7, 'dy': 0, 'color': 'black', 'width': 1.5},
        
        # BG Workers arrows
        {'type': 'arrow', 'x': 8, 'y': 0.4, 'dx': 4, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 19, 'y': 0.4, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 62, 'y': 0.4, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 74, 'y': 0.4, 'dx': 8, 'dy': 0, 'color': 'black', 'width': 1.5},
        
        # io_uring arrows
        {'type': 'arrow', 'x': 8, 'y': 0.1, 'dx': 4, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 18, 'y': 0.1, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 59, 'y': 0.1, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 70, 'y': 0.1, 'dx': 8, 'dy': 0, 'color': 'black', 'width': 1.5},
    ]

    # Draw the main timeline
    draw_timeline(ax, "TPC-H Benchmark Execution Process", blocks, y_positions, test_types)

    # Draw arrows
    for arrow in arrow_elements:
        ax.arrow(arrow['x'], arrow['y'], arrow['dx'], arrow['dy'],
                 head_width=0.02, head_length=2, fc=arrow['color'], ec=arrow['color'],
                 linewidth=arrow['width'])

    # Add legend
    legend_elements = [
        Rectangle((0, 0), 1, 1, facecolor=setup_color, edgecolor='#e67e22', label='PostgreSQL Setup'),
        Rectangle((0, 0), 1, 1, facecolor=refresh_color, edgecolor='#8e44ad', label='Refresh Functions'),
        Rectangle((0, 0), 1, 1, facecolor=power_test_color, edgecolor='#2980b9', label='Power Test'),
        Rectangle((0, 0), 1, 1, facecolor=throughput_test_color, edgecolor='#c0392b', label='Throughput Test'),
    ]

    fig.legend(handles=legend_elements, 
               loc='lower center', 
               bbox_to_anchor=(0.5, 0.02),
               ncol=2, 
               frameon=True,
               fontsize=11,
               fancybox=True,
               shadow=False,
               framealpha=1)

    # Add explanatory text
    ax.text(90, 0.85, "TPC-H Benchmark Execution Flow:\n• Power Test: Sequential Q1-Q22 with RF1/RF2\n• Throughput Test: Parallel query streams with refresh functions\n• Performance improves with advanced I/O methods", 
            ha='center', va='center', fontsize=10, color=text_color,
            bbox=dict(boxstyle="round,pad=0.5", facecolor='lightyellow', edgecolor='orange', alpha=0.8))

    # Add detail about throughput test
    ax.text(130, 0.6, "Throughput Test Details:\n• Multiple query streams in parallel\n• Random query execution order\n• Concurrent refresh functions\n• Measures maximum sustained performance", 
            ha='center', va='center', fontsize=9, color=text_color,
            bbox=dict(boxstyle="round,pad=0.4", facecolor='lightblue', edgecolor='blue', alpha=0.7))

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)  # Make room for legend

    # Save as high-quality PNG
    output_file = 'tpch_benchmark_execution.png'
    plt.savefig(output_file, 
                dpi=300, 
                bbox_inches='tight', 
                facecolor='white',
                edgecolor='none')

    plt.close()
    
    return output_file

if __name__ == "__main__":
    try:
        output_file = create_tpch_benchmark_diagram()
        print(f"✅ TPC-H benchmark execution diagram saved as '{output_file}'")
    except Exception as e:
        print(f"❌ Error creating diagram: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)