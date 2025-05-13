from configparser import ConfigParser
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
import os

def load_table_styles():
    """Load styles from table_format.ini configuration file"""
    config = ConfigParser()
    config.read(os.path.join('config', 'table_format.ini'))
    
    styles = {}
    
    def parse_style(section):
        style = {}
        
        # Parse font
        if config.has_option(section, 'font'):
            font_parts = [p.split('=') for p in config.get(section, 'font').split(', ')]
            font_kwargs = {k.strip(): v.strip() for k, v in font_parts}
            style['font'] = Font(**{
                'name': font_kwargs.get('name', 'Calibri'),
                'bold': font_kwargs.get('bold', 'False') == 'True',
                'size': int(font_kwargs.get('size', 11)),
                'color': font_kwargs.get('color', '000000')
            })
        
        # Parse fill
        if config.has_option(section, 'fill'):
            fill_parts = [p.split('=') for p in config.get(section, 'fill').split(', ')]
            fill_kwargs = {k.strip(): v.strip() for k, v in fill_parts}
            style['fill'] = PatternFill(
                start_color=fill_kwargs.get('start_color', 'FFFFFF'),
                end_color=fill_kwargs.get('end_color', 'FFFFFF'),
                fill_type=fill_kwargs.get('fill_type', 'solid')
            )
        
        # Parse border
        if config.has_option(section, 'border'):
            border_parts = [p.split('=') for p in config.get(section, 'border').split(', ')]
            border_kwargs = {k.strip(): v.strip() for k, v in border_parts}
            border_side = Side(
                border_style=border_kwargs.get('border_style', 'thin'),
                color=border_kwargs.get('color', '000000')
            )
            style['border'] = Border(
                left=border_side,
                right=border_side,
                top=border_side,
                bottom=border_side
            )
        
        # Parse alignment
        if config.has_option(section, 'alignment'):
            align_parts = [p.split('=') for p in config.get(section, 'alignment').split(', ')]
            align_kwargs = {k.strip(): v.strip() for k, v in align_parts}
            style['alignment'] = Alignment(
                horizontal=align_kwargs.get('horizontal', 'left'),
                vertical=align_kwargs.get('vertical', 'center'),
                wrap_text=align_kwargs.get('wrap_text', 'False') == 'True'
            )
        
        # Parse width if exists
        if config.has_option(section, 'width'):
            style['width'] = float(config.get(section, 'width'))
        
        return style
    
    for section in config.sections():
        styles[section] = parse_style(section)
    
    return styles

STYLES = load_table_styles()