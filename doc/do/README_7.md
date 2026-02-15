# GUI Package - Professional UI/UX Components

## Overview

This package provides modern, professional UI components and themes for the Scraper Management System.

## Structure

```
gui/
├── __init__.py              # Package exports
├── README.md                # This file
├── core/                    # Core GUI applications
│   ├── __init__.py
│   ├── main.py             # ProfessionalScraperGUI
│   └── enhanced.py         # EnhancedScraperGUI
├── themes/                  # Color schemes and styling
│   ├── __init__.py
│   ├── modern.py           # ModernTheme, IconLibrary
│   └── styles.py           # apply_modern_styles()
├── components/              # Reusable UI components
│   ├── __init__.py
│   ├── cards.py            # CardFrame, CardGrid
│   ├── buttons.py          # ModernButton, ButtonGroup
│   ├── badges.py           # StatusBadge, LabelBadge
│   ├── inputs.py           # SearchableCombobox, ValidatedEntry
│   └── progress.py         # ProgressIndicator, CircularProgress
└── utils/                   # Helper utilities
    ├── __init__.py
    ├── tooltips.py         # TooltipManager
    ├── notifications.py    # NotificationManager
    ├── shortcuts.py        # KeyboardShortcutManager
    └── animations.py       # AnimationManager
```

## Quick Start

### Run the Professional GUI

```bash
python run_gui_professional.py
```

### Import Components

```python
from gui import ProfessionalScraperGUI
from gui.themes import ModernTheme, IconLibrary
from gui.components import CardFrame, StatusBadge
from gui.utils import TooltipManager, NotificationManager
```

## Components

### CardFrame

Card-style container with title and content area:

```python
from gui.components import CardFrame

card = CardFrame(parent, title="Settings", padding=16)
card.pack(fill=tk.BOTH, expand=True)

# Add content
label = tk.Label(card.content, text="Content")
label.pack()
```

### StatusBadge

Visual status indicator:

```python
from gui.components import StatusBadge

badge = StatusBadge(parent, status='running')
badge.pack(side=tk.LEFT)

# Update status
badge.set_status('stopped')
```

### ModernButton

Styled button with hover effects:

```python
from gui.components import ModernButton

btn = ModernButton(
    parent,
    text="Save",
    icon=IconLibrary.SAVE,
    style='primary',
    command=on_save
)
btn.pack()
```

### ProgressIndicator

Custom progress bar:

```python
from gui.components import ProgressIndicator

progress = ProgressIndicator(parent)
progress.pack(fill=tk.X)

# Update
progress.set_progress(50, "Processing...")
```

### SearchableCombobox

Combobox with real-time search:

```python
from gui.components import SearchableCombobox

combo = SearchableCombobox(
    parent,
    values=['Apple', 'Banana', 'Cherry'],
    on_select=on_select
)
combo.pack()
```

## Utilities

### TooltipManager

Add tooltips to widgets:

```python
from gui.utils import TooltipManager

tooltips = TooltipManager(root)
tooltips.add_tooltip(button, "Click to save")
```

### NotificationManager

Show toast notifications:

```python
from gui.utils import NotificationManager

notifications = NotificationManager(root)
notifications.show("Saved!", level='success')
```

### KeyboardShortcutManager

Manage keyboard shortcuts:

```python
from gui.utils import KeyboardShortcutManager

shortcuts = KeyboardShortcutManager(root)
shortcuts.add_shortcut('<F5>', "Run", on_run)
shortcuts.add_shortcut('<Control-s>', "Save", on_save)
```

## Themes

### ModernTheme

Color palette:

```python
from gui.themes import ModernTheme

colors = ModernTheme.get_all()
bg = colors['BG_CARD']
fg = colors['TEXT_PRIMARY']
```

### IconLibrary

Unicode icons:

```python
from gui.themes import IconLibrary

icon = IconLibrary.SAVE
icon_with_text = IconLibrary.with_text('save', "Save File")
```

## Styling

Apply modern styles to ttk widgets:

```python
from gui.themes import apply_modern_styles

style = ttk.Style()
apply_modern_styles(style)
```

## Keyboard Shortcuts

Default shortcuts in Professional GUI:

| Key | Action |
|-----|--------|
| F1 | Show help |
| F5 | Resume pipeline |
| Ctrl+F5 | Run fresh pipeline |
| F6 | Stop pipeline |
| Ctrl+C | Copy logs |
| Ctrl+S | Save log |
| Ctrl+L | Clear logs |
| Ctrl+R | Refresh outputs |
| Ctrl+Q | Quit |

## Customization

### Change Colors

Edit `gui/themes/modern.py`:

```python
class ModernTheme:
    PRIMARY = "#your-color"
    SUCCESS = "#your-success-color"
```

### Add Icons

Add to `IconLibrary`:

```python
class IconLibrary:
    CUSTOM = "🔥"
```

## Requirements

- Python 3.7+
- tkinter (usually included with Python)

## License

Part of the Scraper Management System.
