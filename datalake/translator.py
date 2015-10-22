import re
import sre_constants
import os


class TranslatorError(Exception):
    pass


class Translator(object):

    def __init__(self, translation_expression):
        '''a translator that translates paths to the specified string

        Args:

        translation_expression: a translation expression of the form:

            <extraction_expression>~<format_expression>

            The `<extraction_expression>` is a regular expression with at least
            one named group. It is separated from the `<format_expression>` by
            the `~` character. The `<format_expression>` is a template
            specifying the desired format. It may contain references to the
            named groups enclosed in braces. Here is an example translation
            expression that will translate the path /var/log/jobs/job-1234.log
            to string job1234:

            '.*job-(?P<job_id>[0-9]+).log$~job{job_id}'
        '''
        self._te = translation_expression
        self._parse_te()

    def _parse_te(self):
        self._validate_te()

    def _validate_te(self):
        self._validate_tilde()
        parts = self._te.split('~')
        self._extract = parts[0]
        self._format = parts[1]
        self._prepare_re()

    def _prepare_re(self):
        try:
            self._re = re.compile(self._extract)
        except sre_constants.error as e:
            raise TranslatorError(e.message)

    def _validate_tilde(self):
        if self._te.count('~') != 1:
            m = ('Translation expression must have exactly one ~ dividing the'
                 'extraction expression from the format expression')
            raise TranslatorError(m)

    def translate(self, path):
        '''apply the translation expression to the specified path

        return the translated string
        '''
        self._validate_path(path)
        matches = self._extract_matches(path)
        if matches is None:
            m = 'Could not match "{}" to "{}"'.format(self._extract, path)
            raise TranslatorError(m)
        return self._apply_format(path, **matches.groupdict())

    def _validate_path(self, path):
        if not os.path.isabs(path):
            m = '{} does not appear to be an absolute path'
            m = m.format(path)
            raise TranslatorError(m)

    def _extract_matches(self, path):
        return self._re.match(path)

    def _apply_format(self, path, **kwargs):
        try:
            return self._format.format(**kwargs)
        except ValueError as e:
            raise TranslatorError(e.message)
        except KeyError as e:
            m = 'Failed to extract "{}" from "{}" using "{}"'
            m = m.format(e.message, path, self._extract)
            raise TranslatorError(m)
