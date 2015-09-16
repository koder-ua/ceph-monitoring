
class RTag(object):
    def __getattr__(self, name):
        def closure(text, **attrs):
            if '_class' in attrs:
                attrs['class'] = attrs.pop('_class')

            if len(attrs) == 0:
                sattrs = ""
            else:
                sattrs = " " + " ".join('{0}="{1}"'.format(name, val) for name, val in attrs.items())

            if text == "" and name not in ('script', 'link'):
                return "<{0}{1} />".format(name, sattrs)
            else:
                return "<{0}{1}>{2}</{0}>".format(name, sattrs, text)
        return closure


class Tag(object):
    def __getattr__(self, name):
        raise AttributeError("Not implemented")


rtag = RTag()
tag = Tag()


class TagProxy(object):
    def __init__(self, doc, name):
        self.__doc = doc
        self.__name = name
        self.__text = ""
        self.__attrs = {}
        self.__childs = []

    def __call__(self, text, **attrs):
        self.__childs.append(text)
        self.__attrs.update(attrs)
        return self

    def __getattr__(self, name):
        tagp = TagProxy(self.__doc, name)
        self.__childs.append(tagp)
        return tagp

    def __enter__(self):
        self.__doc += self
        return self

    def __exit__(self, x, y, z):
        self.__doc -= self

    def __str__(self):
        inner = "".join(map(str, self.__childs))
        return getattr(rtag, self.__name)(inner, **self.__attrs)


class Doc(object):
    def __init__(self):
        self.__stack = []
        self.__childs = []

    def __getattr__(self, name):
        if len(self.__stack) == 0:
            tagp = TagProxy(self, name)
            self.__childs.append(tagp)
        else:
            tagp = getattr(self.__stack[-1], name)
        return tagp

    def __str__(self):
        assert self.__stack == []
        return "".join(map(str, self.__childs))

    def __iadd__(self, tag):
        self.__stack.append(tag)
        return self

    def __isub__(self, tag):
        assert self.__stack.pop() is tag
        return self

    def __call__(self, text, **attrs):
        assert self.__stack != []
        return self.__stack[-1](text, **attrs)


class HTMLTable(object):
    def_table_attrs = {
        'class': 'table table-bordered table-striped table-condensed sortable'
    }

    def __init__(self, headers, table_attrs=def_table_attrs):
        self.table_attrs = table_attrs
        self.headers = [(header, {}) for header in headers]
        self.allign = ['center'] * len(self.headers)
        self.cells = [[]]

    def add_header(self, text, attrs=None):
        self.headers.append((text, attrs))

    def add_cell(self, data, **attrs):
        self.cells[-1].append((data, attrs))

    def add_row(self, data, **attrs):
        for val in data:
            self.add_cell(val, **attrs)
        self.next_row()

    def next_row(self):
        self.cells.append([])

    def __str__(self):
        t = Doc()

        with t.table('', **self.table_attrs):
            with t.thead.tr:
                for header, attrs in self.headers:
                    t.th(header, **attrs)

            with t.tbody:
                for line in self.cells:
                    with t.tr:
                        for cell, attrs in line:
                            t.td(cell, **attrs)

        return str(t)
