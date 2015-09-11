class HTMLTable(object):
    def_table_attrs = {
        'cellpadding': '4',
        'style': 'border: 1px solid #000000; border-collapse: collapse;',
        'border': '1'
    }

    def __init__(self, headers, table_attrs=def_table_attrs):
        self.table_attrs = table_attrs
        self.headers = [(header, {}) for header in headers]
        self.allign = ['center'] * len(self.headers)
        self.cells = [[]]

    def add_header(self, text, attrs=None):
        self.headers.append((text, attrs if attrs is not None else {}))

    def add_cell(self, data, attrs=None):
        self.cells[-1].append((data, attrs if attrs is not None else {}))

    def add_row(self, data, attrs=None):
        for val in data:
            self.add_cell(val, attrs)
        self.next_row()

    def next_row(self):
        self.cells.append([])

    def __str__(self):
        def attrs2str(attrs):
            if len(attrs) == 0:
                return ""
            return " " + " ".join('{0}="{1}"'.format(name, val) for name, val in attrs.items())

        res = "<table{0}>\n".format(attrs2str(self.table_attrs))
        res += '    <tr>'
        for header, attrs in self.headers:
            res += "<th{0}>{1}</th>".format(attrs2str(attrs), header)
        res += "</tr>\n"

        for line in self.cells:
            if len(line) == 0:
                continue
            res += "    <tr>"
            for cell, attrs in line:
                res += "<td{0}>{1}</td>".format(attrs2str(attrs), cell)
            res += "</tr>\n"

        return res + "</table>\n"
