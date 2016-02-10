.. _{{ fullname }}:

{{ name }}
{{ underline }}

.. currentmodule:: {{ fullname }}
.. automodule:: {{ fullname }}

    {% block functions %}
    {% if functions %}
    .. raw:: html

        <h1>Functions</h1>
        <hr>

    {% for item in functions %}
    .. autofunction:: {{ item }}
    .. raw:: html

        <hr>

    {%- endfor %}
    {% endif %}
    {% endblock %}

    {% block classes %}
    {% if classes %}
    .. raw:: html

        <h1>Classes</h1>
        <hr>

    {% for item in classes %}
    .. autoclass:: {{ item }}
        :members:
        :special-members:

    .. raw:: html

        <hr>

    {%- endfor %}
    {% endif %}
    {% endblock %}

    {% block exceptions %}
    {% if exceptions %}
    .. raw:: html

        <h1>Exceptions</h1>
        <hr>

    {% for item in exceptions %}
    .. autosummary:: {{ item }}
    .. raw:: html

        <hr>

    {%- endfor %}
    {% endif %}
    {% endblock %}
