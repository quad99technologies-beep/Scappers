# Bohrium-style Layout Wrapper for Existing ScraperGUI
# Same colors, fonts, and business logic preserved
# ONLY layout restructured: Sidebar + Page Frames

import tkinter as tk
from tkinter import ttk
from scraper_gui import ScraperGUI   # <-- your original file


class BohriumShell(ScraperGUI):
    def setup_ui(self):
        self.setup_styles()

        # Root container
        root_container = tk.Frame(self.root, bg=self.colors['background_gray'])
        root_container.pack(fill=tk.BOTH, expand=True)

        # ---------------- Sidebar ----------------
        self.sidebar = tk.Frame(root_container, bg=self.colors['background_gray'], width=220)
        self.sidebar.pack(side=tk.LEFT, fill=tk.Y)
        self.sidebar.pack_propagate(False)

        tk.Label(
            self.sidebar,
            text="Bohrium",
            bg=self.colors['background_gray'],
            fg=self.colors['text_black'],
            font=self.fonts['bold']
        ).pack(pady=20)

        self.nav_buttons = {}

        def nav_button(name):
            b = ttk.Button(
                self.sidebar,
                text=name,
                style='Secondary.TButton',
                command=lambda: self.show_page(name)
            )
            b.pack(fill=tk.X, padx=12, pady=4)
            self.nav_buttons[name] = b

        nav_button("Dashboard")
        nav_button("Output")
        nav_button("Health Check")
        nav_button("Pipeline")
        nav_button("Documentation")

        # ---------------- Content Area ----------------
        self.content = tk.Frame(root_container, bg=self.colors['white'])
        self.content.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.pages = {}

        def create_page(name):
            frame = ttk.Frame(self.content)
            frame.pack(fill=tk.BOTH, expand=True)
            self.pages[name] = frame
            return frame

        dashboard = create_page("Dashboard")
        output = create_page("Output")
        health = create_page("Health Check")
        pipeline = create_page("Pipeline")
        docs = create_page("Documentation")

        # ---------------- Reuse Existing Builders ----------------
        self.setup_dashboard_page(dashboard)
        self.setup_outputs_page(output)
        self.setup_health_check_tab(health)
        self.setup_pipeline_steps_tab(pipeline)
        self.setup_documentation_tab(docs)

        self.show_page("Dashboard")

    def show_page(self, name):
        for p in self.pages.values():
            p.pack_forget()
        self.pages[name].pack(fill=tk.BOTH, expand=True)


# ---------------- App Entry ----------------
if __name__ == "__main__":
    root = tk.Tk()
    app = BohriumShell(root)
    root.mainloop()
