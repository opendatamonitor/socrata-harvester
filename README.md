socrata-harvester
=================

A harvester to allow CKAN directories to keep in sync with a catalogue that provides API in order to fetch metadata.

In order to use this tool, you need to have the ODM CKAN harvester extension (https://github.com/opendatamonitor/ckanext-harvestodm)
installed and loaded for your CKAN instance.
Tested with CKAN v2.2 (http://docs.ckan.org/en/ckan-2.2/).


General
---------

This work is based on the socrata harvester extension (https://github.com/socrata/socrata-harvester).
The socrata-harvester plugin adds support in using the mongo DB as metadata repository. Also, changes or modifications added to original
code to comply with ODM project's (www.opendatamonitor.eu) requirements (see below).


Implementation
---------------

Main modifications are:

* add extra metadata fields (language, country, catalogue_url, platform) or use existing ones in different way (metadata_created and metadata_updated are synchronised 
to our platform's timings overriding the client's) check whether a metadata record is already present in the MongoDB database, and accordingly create or update

Building
---------

To build and use this plugin, simply:

    git clone https://github.com/opendatamonitor/socrata-harvester.git
    cd socrata-harvester
    pip install -r pip-requirements.txt
    python setup.py develop

Then you will need to update your CKAN configuration to include the new harvester.  This will mean adding the
socrata_harvester plugin as a plugin.  E.g.

    ckan.plugins = harvestodm socrata_harvest

    
Using
---------

After setting this up, you should be able to go to:
    http://localhost:5000/harvest

And have a new "Socrata" harvest type show up when creating sources.


Licence
---------

This work implements the ckanext-harvest template (https://github.com/ckan/ckanext-harvest) and thus 
licensed under the GNU Affero General Public License (AGPL) v3.0 (http://www.fsf.org/licensing/licenses/agpl-3.0.html).
