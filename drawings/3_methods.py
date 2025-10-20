import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Rectangle

# Set style for academic paper
plt.style.use('default')
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), dpi=300)

# Colors for consistent styling
cpu_color = '#3498db'
io_color = '#e74c3c'
bg_worker_color = '#9b59b6'
completion_color = '#2ecc71'
text_color = '#2c3e50'

def draw_timeline(ax, title, blocks, y_positions, thread_labels=None, method_specific=None):
    """Draw a timeline with CPU and I/O blocks for different threads"""
    ax.set_xlim(0, 180)
    ax.set_ylim(0, 1)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20, color=text_color)
    
    # Draw timelines for each thread
    for i, y_pos in enumerate(y_positions):
        ax.axhline(y=y_pos, color='#7f8c8d', linewidth=1.5, alpha=0.7)
        if thread_labels:
            ax.text(-15, y_pos, thread_labels[i], ha='right', va='center', 
                    fontweight='bold', fontsize=10, color=text_color)
    
    # Draw blocks
    for block in blocks:
        if block['type'] == 'cpu':
            color = cpu_color
            edgecolor = '#2980b9'
        elif block['type'] == 'io':
            color = io_color
            edgecolor = '#c0392b'
        elif block['type'] == 'bg_worker':
            color = bg_worker_color
            edgecolor = '#8e44ad'
        elif block['type'] == 'completion':
            color = completion_color
            edgecolor = '#27ae60'
        else:
            color = '#bdc3c7'
            edgecolor = '#95a5a6'
        
        rect = Rectangle((block['start'], block['y']-0.1), 
                        block['duration'], 0.2,
                        facecolor=color, edgecolor=edgecolor, linewidth=1.2,
                        alpha=0.9)
        ax.add_patch(rect)
        
        # Add label ABOVE the block (not inside) for better visibility
        label_y = block['y'] + 0.15  # Position above the block
        ax.text(block['start'] + block['duration']/2, label_y, 
                block['label'], ha='center', va='center', 
                fontweight='bold', color=text_color, fontsize=9,
                bbox=dict(boxstyle="round,pad=0.2", facecolor='white', edgecolor='none', alpha=0.8))
    
    # Draw method-specific elements
    if method_specific:
        for element in method_specific:
            if element['type'] == 'arrow':
                ax.arrow(element['x'], element['y'], element['dx'], element['dy'],
                        head_width=0.02, head_length=3, fc=element['color'], ec=element['color'])
            elif element['type'] == 'bracket':
                # Draw a simple bracket using lines
                x, y, width = element['x'], element['y'], element['width']
                if element['orientation'] == 'down':
                    ax.plot([x, x], [y, y-0.05], color=element['color'], linewidth=2)
                    ax.plot([x+width, x+width], [y, y-0.05], color=element['color'], linewidth=2)
                    ax.plot([x, x+width], [y-0.05, y-0.05], color=element['color'], linewidth=2)

    # Remove axes
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)

# 1. SYNCHRONOUS I/O
sync_blocks = [
    {'type': 'cpu', 'start': 0, 'duration': 15, 'y': 0.7, 'label': 'CPU'},
    {'type': 'io', 'start': 15, 'duration': 35, 'y': 0.7, 'label': 'I/O 1'},
    {'type': 'cpu', 'start': 50, 'duration': 15, 'y': 0.7, 'label': 'CPU'},
    {'type': 'io', 'start': 65, 'duration': 35, 'y': 0.7, 'label': 'I/O 2'},
    {'type': 'cpu', 'start': 100, 'duration': 15, 'y': 0.7, 'label': 'CPU'},
    {'type': 'io', 'start': 115, 'duration': 35, 'y': 0.7, 'label': 'I/O 3'},
    {'type': 'cpu', 'start': 150, 'duration': 15, 'y': 0.7, 'label': 'CPU'}
]
draw_timeline(ax1, "1. Synchronous I/O", sync_blocks, [0.7], ["Main Thread"])

# 2. BACKGROUND WORKERS
bg_blocks = [
    # Main thread
    {'type': 'cpu', 'start': 0, 'duration': 10, 'y': 0.8, 'label': 'Submit'},
    {'type': 'cpu', 'start': 45, 'duration': 10, 'y': 0.8, 'label': 'Submit'},
    {'type': 'cpu', 'start': 90, 'duration': 10, 'y': 0.8, 'label': 'Submit'},
    {'type': 'cpu', 'start': 135, 'duration': 25, 'y': 0.8, 'label': 'Process Results'},
    
    # Background workers
    {'type': 'bg_worker', 'start': 10, 'duration': 30, 'y': 0.5, 'label': 'I/O 1'},
    {'type': 'bg_worker', 'start': 55, 'duration': 30, 'y': 0.5, 'label': 'I/O 2'},
    {'type': 'bg_worker', 'start': 100, 'duration': 30, 'y': 0.5, 'label': 'I/O 3'},
    
    # Completion
    {'type': 'completion', 'start': 40, 'duration': 5, 'y': 0.8, 'label': 'Done'},
    {'type': 'completion', 'start': 85, 'duration': 5, 'y': 0.8, 'label': 'Done'},
    {'type': 'completion', 'start': 130, 'duration': 5, 'y': 0.8, 'label': 'Done'}
]

bg_elements = [
    {'type': 'arrow', 'x': 10, 'y': 0.75, 'dx': 0, 'dy': -0.2, 'color': bg_worker_color},
    {'type': 'arrow', 'x': 55, 'y': 0.75, 'dx': 0, 'dy': -0.2, 'color': bg_worker_color},
    {'type': 'arrow', 'x': 100, 'y': 0.75, 'dx': 0, 'dy': -0.2, 'color': bg_worker_color},
    {'type': 'arrow', 'x': 40, 'y': 0.55, 'dx': 0, 'dy': 0.2, 'color': completion_color},
    {'type': 'arrow', 'x': 85, 'y': 0.55, 'dx': 0, 'dy': 0.2, 'color': completion_color},
    {'type': 'arrow', 'x': 130, 'y': 0.55, 'dx': 0, 'dy': 0.2, 'color': completion_color}
]

draw_timeline(ax2, "2. Background Workers", bg_blocks, [0.8, 0.5], ["Main Thread", "BG Workers"], bg_elements)

# 3. IO_URING
uring_blocks = [
    # Main thread - continuous processing
    {'type': 'cpu', 'start': 0, 'duration': 140, 'y': 0.8, 'label': 'Continuous CPU'},
    
    # Submission queue
    {'type': 'cpu', 'start': 5, 'duration': 5, 'y': 0.6, 'label': 'SQE 1'},
    {'type': 'cpu', 'start': 25, 'duration': 5, 'y': 0.6, 'label': 'SQE 2'},
    {'type': 'cpu', 'start': 45, 'duration': 5, 'y': 0.6, 'label': 'SQE 3'},
    
    # Completion queue
    {'type': 'completion', 'start': 30, 'duration': 5, 'y': 0.4, 'label': 'CQE 1'},
    {'type': 'completion', 'start': 50, 'duration': 5, 'y': 0.4, 'label': 'CQE 2'},
    {'type': 'completion', 'start': 70, 'duration': 5, 'y': 0.4, 'label': 'CQE 3'},
    
    # Kernel I/O (happening asynchronously)
    {'type': 'io', 'start': 10, 'duration': 20, 'y': 0.2, 'label': 'I/O 1'},
    {'type': 'io', 'start': 30, 'duration': 20, 'y': 0.2, 'label': 'I/O 2'},
    {'type': 'io', 'start': 50, 'duration': 20, 'y': 0.2, 'label': 'I/O 3'}
]

uring_elements = [
    # Submission arrows
    {'type': 'arrow', 'x': 10, 'y': 0.58, 'dx': 0, 'dy': -0.15, 'color': cpu_color},
    {'type': 'arrow', 'x': 30, 'y': 0.58, 'dx': 0, 'dy': -0.15, 'color': cpu_color},
    {'type': 'arrow', 'x': 50, 'y': 0.58, 'dx': 0, 'dy': -0.15, 'color': cpu_color},
    # Completion arrows
    {'type': 'arrow', 'x': 30, 'y': 0.42, 'dx': 0, 'dy': 0.15, 'color': completion_color},
    {'type': 'arrow', 'x': 50, 'y': 0.42, 'dx': 0, 'dy': 0.15, 'color': completion_color},
    {'type': 'arrow', 'x': 70, 'y': 0.42, 'dx': 0, 'dy': 0.15, 'color': completion_color},
    # Kernel processing bracket
    {'type': 'bracket', 'x': 0, 'y': 0.1, 'width': 180, 'orientation': 'down', 'color': io_color}
]

draw_timeline(ax3, "3. io_uring", uring_blocks, [0.8, 0.6, 0.4, 0.2], 
              ["Main Thread", "Submission Queue", "Completion Queue", "Kernel I/O"], 
              uring_elements)

# Add kernel label for io_uring
ax3.text(90, 0.1, "Kernel Space", ha='center', va='center', 
         fontweight='bold', fontsize=10, color=text_color,
         bbox=dict(boxstyle="round,pad=0.3", facecolor='white', edgecolor=io_color))

# Add legend
legend_elements = [
    Rectangle((0, 0), 1, 1, facecolor=cpu_color, edgecolor='#2980b9', label='CPU Processing'),
    Rectangle((0, 0), 1, 1, facecolor=io_color, edgecolor='#c0392b', label='I/O Operation'),
    Rectangle((0, 0), 1, 1, facecolor=bg_worker_color, edgecolor='#8e44ad', label='Background Worker'),
    Rectangle((0, 0), 1, 1, facecolor=completion_color, edgecolor='#27ae60', label='Completion')
]

fig.legend(handles=legend_elements, 
           loc='lower center', 
           bbox_to_anchor=(0.5, 0.02),
           ncol=4, 
           frameon=True,
           fontsize=11,
           fancybox=True,
           shadow=False,
           framealpha=1)

plt.tight_layout()
plt.subplots_adjust(bottom=0.12, hspace=0.8)  # Make room for legend

# Save as high-quality PNG for paper
plt.savefig('three_methods.png', 
            dpi=300, 
            bbox_inches='tight', 
            facecolor='white',
            edgecolor='none')

plt.show()

print("Fixed diagram saved as 'three_methods.png'")