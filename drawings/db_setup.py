#!/usr/bin/env python3
"""
TPC-H Database Creation Diagram Generator
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

def create_tpch_diagram():
    """Create TPC-H database creation timeline diagram"""
    
    # Set style for academic paper
    plt.style.use('default')
    fig, ax = plt.subplots(1, 1, figsize=(14, 8), dpi=300)

    # Colors for consistent styling
    download_color = '#3498db'
    dbgen_color = '#9b59b6'
    load_color = '#2ecc71'
    schema_color = '#f39c12'
    text_color = '#2c3e50'

    def draw_timeline(ax, title, blocks, y_positions, scale_factors):
        """Draw a timeline showing database creation process for different scale factors"""
        ax.set_xlim(0, 180)
        ax.set_ylim(0, 1)
        ax.set_title(title, fontsize=16, fontweight='bold', pad=20, color=text_color)
        
        # Draw timelines for each scale factor
        for i, y_pos in enumerate(y_positions):
            ax.axhline(y=y_pos, color='#7f8c8d', linewidth=1.5, alpha=0.7)
            # Fixed: Use scale factor label directly
            ax.text(-18, y_pos, "Scale Factor:", ha='right', va='center', 
                    fontweight='bold', fontsize=11, color=text_color)
        
        # Draw scale factor labels on the right
        for i, sf in enumerate(scale_factors):
            ax.text(185, y_positions[i], sf, ha='left', va='center', 
                    fontweight='bold', fontsize=10, color=text_color,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor='lightgray', alpha=0.7))
        
        # Draw blocks
        for block in blocks:
            if block['type'] == 'download':
                color = download_color
                edgecolor = '#2980b9'
            elif block['type'] == 'dbgen':
                color = dbgen_color
                edgecolor = '#8e44ad'
            elif block['type'] == 'schema':
                color = schema_color
                edgecolor = '#e67e22'
            elif block['type'] == 'load':
                color = load_color
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
            {'name': 'Setup', 'start': 0, 'end': 20},
            {'name': 'Generate', 'start': 20, 'end': 120},
            {'name': 'Load', 'start': 120, 'end': 180}
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

    # Database creation timeline for different scale factors
    scale_factors = ['1 GB', '10 GB', '100 GB']
    y_positions = [0.7, 0.4, 0.1]

    # Blocks for different scale factors with increasing durations (Build removed)
    blocks = [
        # 1GB scale factor
        {'type': 'download', 'start': 5, 'duration': 10, 'y': 0.7, 'label': 'Download\ntpch-kit'},
        {'type': 'dbgen', 'start': 20, 'duration': 40, 'y': 0.7, 'label': 'Generate\nData'},
        {'type': 'schema', 'start': 125, 'duration': 8, 'y': 0.7, 'label': 'Create\nSchema'},
        {'type': 'load', 'start': 138, 'duration': 40, 'y': 0.7, 'label': 'Load\nData'},
        
        # 10GB scale factor (takes longer)
        {'type': 'download', 'start': 5, 'duration': 10, 'y': 0.4, 'label': 'Download\ntpch-kit'},
        {'type': 'dbgen', 'start': 20, 'duration': 80, 'y': 0.4, 'label': 'Generate\nData'},
        {'type': 'schema', 'start': 125, 'duration': 8, 'y': 0.4, 'label': 'Create\nSchema'},
        {'type': 'load', 'start': 138, 'duration': 40, 'y': 0.4, 'label': 'Load\nData'},
        
        # 100GB scale factor (takes much longer)
        {'type': 'download', 'start': 5, 'duration': 10, 'y': 0.1, 'label': 'Download\ntpch-kit'},
        {'type': 'dbgen', 'start': 20, 'duration': 100, 'y': 0.1, 'label': 'Generate\nData'},
        {'type': 'schema', 'start': 125, 'duration': 8, 'y': 0.1, 'label': 'Create\nSchema'},
        {'type': 'load', 'start': 138, 'duration': 40, 'y': 0.1, 'label': 'Load\nData'},
    ]

    # Add arrows showing progression between phases for ALL scale factors
    arrow_elements = [
        # 1GB arrows
        {'type': 'arrow', 'x': 15, 'y': 0.7, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 60, 'y': 0.7, 'dx': 65, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 133, 'y': 0.7, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        
        # 10GB arrows
        {'type': 'arrow', 'x': 15, 'y': 0.4, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 100, 'y': 0.4, 'dx': 25, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 133, 'y': 0.4, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        
        # 100GB arrows
        {'type': 'arrow', 'x': 15, 'y': 0.1, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 120, 'y': 0.1, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
        {'type': 'arrow', 'x': 133, 'y': 0.1, 'dx': 5, 'dy': 0, 'color': 'black', 'width': 1.5},
    ]

    # FIXED: Remove thread_labels parameter
    draw_timeline(ax, "TPC-H Database Creation Process", blocks, y_positions, scale_factors)

    # Draw arrows
    for arrow in arrow_elements:
        ax.arrow(arrow['x'], arrow['y'], arrow['dx'], arrow['dy'],
                 head_width=0.02, head_length=2, fc=arrow['color'], ec=arrow['color'],
                 linewidth=arrow['width'])

    # Add legend (Build removed)
    legend_elements = [
        Rectangle((0, 0), 1, 1, facecolor=download_color, edgecolor='#2980b9', label='Download'),
        Rectangle((0, 0), 1, 1, facecolor=dbgen_color, edgecolor='#8e44ad', label='Data Generation'),
        Rectangle((0, 0), 1, 1, facecolor=schema_color, edgecolor='#e67e22', label='Schema Creation'),
        Rectangle((0, 0), 1, 1, facecolor=load_color, edgecolor='#27ae60', label='Data Loading'),
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
    ax.text(90, 0.85, "Time required increases with scale factor:\n• Data generation takes progressively longer\n• Setup time remains relatively constant", 
            ha='center', va='center', fontsize=10, color=text_color,
            bbox=dict(boxstyle="round,pad=0.5", facecolor='lightyellow', edgecolor='orange', alpha=0.8))

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)  # Make room for legend

    # Save as high-quality PNG
    output_file = 'tpch_database_creation.png'
    plt.savefig(output_file, 
                dpi=300, 
                bbox_inches='tight', 
                facecolor='white',
                edgecolor='none')

    plt.close()
    
    return output_file

if __name__ == "__main__":
    try:
        output_file = create_tpch_diagram()
        print(f"✅ TPC-H database creation diagram saved as '{output_file}'")
    except Exception as e:
        print(f"❌ Error creating diagram: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)