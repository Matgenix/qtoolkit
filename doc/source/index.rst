.. _qtoolkit_docs_mainpage:

######################
QToolKit documentation
######################

.. toctree::
   :maxdepth: 1
   :hidden:

   User Guide <user/index>
   API reference <api/index>
   Development <dev/index>
   Changelog <changelog>


**Version**: |version|

QToolKit is an interface to Distributed Resource Management (DRM) systems, e.g. SLURM and PBS, with the aim to enable programmatic control of queuing systems.


.. grid:: 1 2 2 2

    .. grid-item-card::
        :img-top: ../source/_static/index-images/getting_started.svg

        Getting Started
        ^^^^^^^^^^^^^^^

        If you want to get started quickly, check out our quickstart section.
        It contains an introduction to QToolKit's main concepts.

        +++

        .. button-ref:: user/quickstart
            :expand:
            :color: secondary
            :click-parent:

            Quickstart

    .. grid-item-card::
        :img-top: ../source/_static/index-images/user_guide.svg

        User Guide
        ^^^^^^^^^^

        The user guide provides in-depth information on the
        key concepts of QToolKit with useful background information and explanation.

        +++

        .. button-ref:: user_guide
            :expand:
            :color: secondary
            :click-parent:

            User Guide

    .. grid-item-card::
        :img-top: ../source/_static/index-images/api.svg

        API Reference
        ^^^^^^^^^^^^^

        The reference guide contains a detailed description of the functions,
        modules, and objects included in QToolKit. The reference describes how the
        methods work and which parameters can be used. It assumes that you have an
        understanding of the key concepts.

        +++

        .. button-ref:: api
            :expand:
            :color: secondary
            :click-parent:

            API Reference

    .. grid-item-card::
        :img-top: ../source/_static/index-images/contributor.svg

        Contributor's Guide
        ^^^^^^^^^^^^^^^^^^^

        Want to add to the codebase? Can help add support to an additional DRM system?
        The contributing guidelines will guide you through the
        process of improving QToolKit.

        +++

        .. button-ref:: devindex
            :expand:
            :color: secondary
            :click-parent:

            To the contributor's guide

.. This is not really the index page, that is found in
   _templates/indexcontent.html The toctree content here will be added to the
   top of the template header
