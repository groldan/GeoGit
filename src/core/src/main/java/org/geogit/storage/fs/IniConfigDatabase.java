/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.fs;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.geogit.api.Platform;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.geogit.api.porcelain.ConfigException;
import org.geogit.api.porcelain.ConfigException.StatusCode;
import org.geogit.storage.ConfigDatabase;
import org.ini4j.Profile.Section;
import org.ini4j.Wini;

import com.google.common.base.Optional;
import com.google.inject.Inject;

/**
 * Provides a means of managing GeoGit configuration options through the use of an INI file.
 * 
 * @see ConfigDatabase
 */
public class IniConfigDatabase implements ConfigDatabase {

    final private Platform platform;

    private WiniCache localWini;

    private File lastKnownWorkingDirectory;

    private WiniCache globalWini;

    private File lastKnownHomeDir;

    /**
     * Constructs a new {@code IniConfigDatabase} with the given platform.
     * 
     * @param platform the platform to use
     */
    @Inject
    public IniConfigDatabase(Platform platform) {
        this.platform = platform;
    }

    private class SectionOptionPair {
        String section;

        String option;

        public SectionOptionPair(String key) {
            final int index = key.lastIndexOf('.');

            if (index == -1) {
                throw new ConfigException(StatusCode.SECTION_OR_KEY_INVALID);
            }

            section = key.substring(0, index);
            option = key.substring(index + 1);

            if (section.length() == 0 || option.length() == 0) {
                throw new ConfigException(StatusCode.SECTION_OR_KEY_INVALID);
            }
        }
    }

    private Wini config() {
        if (localWini == null || !lastKnownWorkingDirectory.equals(platform.pwd())) {
            final Optional<URL> url = new ResolveGeogitDir(platform).call();

            if (!url.isPresent()) {
                throw new ConfigException(StatusCode.INVALID_LOCATION);
            }

            /*
             * See http://weblogs.java.net/blog/kohsuke/archive/2007/04/how_to_convert.html for
             * explanation on this idiom.
             */
            File localConfigFile;
            try {
                localConfigFile = new File(new File(url.get().toURI()), "config");
            } catch (URISyntaxException e) {
                localConfigFile = new File(url.get().getPath(), "config");
            }

            lastKnownWorkingDirectory = platform.pwd();
            localWini = new WiniCache(localConfigFile);
        }
        return localWini.getWini();
    }

    private Wini globalConfig() {
        if (globalWini == null || !lastKnownHomeDir.equals(platform.getUserHome())) {
            File home = platform.getUserHome();

            if (home == null) {
                throw new ConfigException(StatusCode.USERHOME_NOT_SET);
            }

            File globalConfig = new File(home.getPath(), ".geogitconfig");
            try {
                globalConfig.createNewFile();
            } catch (IOException e) {
                throw new ConfigException(e, StatusCode.CANNOT_WRITE);
            }
            this.lastKnownHomeDir = home;
            this.globalWini = new WiniCache(globalConfig);
        }
        return globalWini.getWini();
    }

    private <T> Optional<T> get(String key, Wini ini, Class<T> c) {
        if (key == null) {
            throw new ConfigException(StatusCode.SECTION_OR_NAME_NOT_PROVIDED);
        }

        final SectionOptionPair pair = new SectionOptionPair(key);
        try {
            T value = ini.get(pair.section.replace(".", "\\"), pair.option, c);

            if (value == null)
                return Optional.absent();

            if (c == String.class && ((String) value).length() == 0)
                return Optional.absent();

            return Optional.of(value);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e, StatusCode.INVALID_LOCATION);
        }
    }

    private Map<String, String> getAll(Wini ini) {
        try {

            Map<String, String> results = new LinkedHashMap<String, String>();

            for (Entry<String, Section> section : ini.entrySet()) {
                if (section.getValue().getParent() == null) {
                    getFromSection(section.getValue(), section.getKey(), results);
                }
            }

            return results;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e, StatusCode.INVALID_LOCATION);
        }
    }

    private Map<String, String> getAllSection(String section, Wini ini) {
        try {

            Map<String, String> results = new LinkedHashMap<String, String>();

            Section iniSection = ini.get(section);
            getFromSection(iniSection, section, results);

            return results;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e, StatusCode.INVALID_LOCATION);
        }
    }

    private void getFromSection(Section section, String sectionKey, Map<String, String> map) {
        if (section != null) {
            for (String optionKey : section.keySet()) {
                map.put(sectionKey + "." + optionKey, section.get(optionKey));
            }
            String[] children = section.childrenNames();
            for (int i = 0; i < children.length; i++) {
                Section subSection = section.getChild(children[i]);
                String sectionName = sectionKey + "." + children[i];
                getFromSection(subSection, sectionName, map);
            }
        }
    }

    private List<String> getAllSubsections(String section, Wini ini) {
        try {
            List<String> results = null;

            Section iniSection = ini.get(section);
            if (iniSection != null) {
                results = new ArrayList<String>(Arrays.asList(iniSection.childrenNames()));
            }

            if (results == null || results.isEmpty())
                return new ArrayList<String>();

            return results;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e, StatusCode.INVALID_LOCATION);
        }
    }

    private void put(String key, Object value, Wini ini) {
        final SectionOptionPair pair = new SectionOptionPair(key);
        try {
            String[] sections = pair.section.split("\\.");
            Section section = ini.get(sections[0]);
            if (section == null) {
                section = ini.add(sections[0]);
            }
            for (int i = 1; i < sections.length; i++) {
                Section childSection = section.getChild(sections[i]);
                if (childSection == null) {
                    childSection = section.addChild(sections[i]);
                }
                section = childSection;
            }
            section.put(pair.option, value);
            // ini.put(pair.section, pair.option, value);
            ini.store();
        } catch (Exception e) {
            throw new ConfigException(e, StatusCode.INVALID_LOCATION);
        }
    }

    private void remove(String key, Wini ini) {
        final SectionOptionPair pair = new SectionOptionPair(key);
        try {
            ini.remove(pair.section.replace(".", "\\"), pair.option);
            ini.store();
        } catch (Exception e) {
            throw new ConfigException(e, StatusCode.INVALID_LOCATION);
        }
    }

    private void removeSection(String key, Wini ini) {
        Section sectionToRemove = ini.get(key.replace(".", "\\"));

        if (sectionToRemove == null)
            throw new ConfigException(StatusCode.MISSING_SECTION);

        ini.remove(sectionToRemove);

        try {
            ini.store();
        } catch (Exception e) {
            throw new ConfigException(e, StatusCode.INVALID_LOCATION);
        }
    }

    /**
     * Queries the repository config file for a particular name.
     * 
     * @param key String in "section.key" format to query for
     * @return The value of the key if found, otherwise an empty Optional
     * @throws ConfigException if an error is encountered
     */
    @Override
    public Optional<String> get(String key) {
        return get(key, config(), String.class);
    }

    /**
     * Queries the global config file for a particular name.
     * 
     * @param key String in "section.key" format to query for
     * @return The value of the key if found, otherwise an empty Optional
     * @throws ConfigException if an error is encountered
     */
    @Override
    public Optional<String> getGlobal(String key) {
        return get(key, globalConfig(), String.class);
    }

    /**
     * Queries the repository config file for a particular name.
     * 
     * @param key String in "section.key" format to query for
     * @param c The type to return the value as
     * @return The value of the key if found, otherwise an empty Optional
     * @throws IllegalArgumentException if unable to return value as type c
     * @throws ConfigException if an error is encountered
     */
    @Override
    public <T> Optional<T> get(String key, Class<T> c) {
        return get(key, config(), c);
    }

    /**
     * Queries the global config file for a particular name.
     * 
     * @param key String in "section.key" format to query for
     * @param c The type to return the value as
     * @return The value of the key if found, otherwise an empty Optional
     * @throws IllegalArgumentException if unable to return value as type c
     * @throws ConfigException if an error is encountered
     */
    @Override
    public <T> Optional<T> getGlobal(String key, Class<T> c) {
        return get(key, globalConfig(), c);
    }

    /**
     * Builds and returns a map with all of the values from the repository config file.
     * 
     * @return A map which contains all of the contents of the config file.
     * @throws ConfigException if an error is encountered
     */
    @Override
    public Map<String, String> getAll() {
        return getAll(config());
    }

    /**
     * Builds and returns a map with all of the values from the global config file.
     * 
     * @return A map which contains all of the contents of the config file.
     * @throws ConfigException if an error is encountered
     */
    @Override
    public Map<String, String> getAllGlobal() {
        return getAll(globalConfig());
    }

    /**
     * Builds and returns a map with all of the values of a specific section from the local config
     * file.
     * 
     * @return A map which contains all of the contents of a particular section of the config file.
     * @throws ConfigException if an error is encountered
     */
    @Override
    public Map<String, String> getAllSection(String section) {
        return getAllSection(section, config());
    }

    /**
     * Builds and returns a map with all of the values of a specific section from the global config
     * file.
     * 
     * @return A map which contains all of the contents of a particular section of the config file.
     * @throws ConfigException if an error is encountered
     */
    @Override
    public Map<String, String> getAllSectionGlobal(String section) {
        return getAllSection(section, globalConfig());
    }

    /**
     * Builds and returns a list of all subsections of the given section from the local config file.
     * 
     * @return A list which contains all of the subsections of the given section.
     * @throws ConfigException if an error is encountered
     */
    public List<String> getAllSubsections(String section) {
        return getAllSubsections(section, config());
    }

    /**
     * Builds and returns a list of all subsections of the given section from the global config
     * file.
     * 
     * @return A list which contains all of the subsections of the given section.
     * @throws ConfigException if an error is encountered
     */
    public List<String> getAllSubsectionsGlobal(String section) {
        return getAllSubsections(section, globalConfig());
    }

    /**
     * Sets a value in the repository config file, e.g. put("section.key", "value") Resulting config
     * file would look like: [section] key = value
     * 
     * @param key String in "section.key" format to set
     * @param value The value to set
     * @throws ConfigException if an error is encountered
     */
    @Override
    public void put(String key, Object value) {
        put(key, value, config());
    }

    /**
     * Sets a value in the global config file
     * 
     * @param key String in "section.key" format to set
     * @param value The value to set
     * @throws ConfigException if an error is encountered
     */
    @Override
    public void putGlobal(String key, Object value) {
        put(key, value, globalConfig());
    }

    /**
     * Removes a value from the repository config file
     * 
     * @param key String in "section.key" format to set
     * @throws ConfigException if an error is encountered
     */
    @Override
    public void remove(String key) {
        remove(key, config());
    }

    /**
     * Removes a value from the global config file
     * 
     * @param key String in "section.key" format to set
     * @throws ConfigException if an error is encountered
     */
    @Override
    public void removeGlobal(String key) {
        remove(key, globalConfig());
    }

    /**
     * Removes a section from the repository config file
     * 
     * @param key String in "section" format to set
     * @throws ConfigException if an error is encountered
     */
    @Override
    public void removeSection(String key) {
        removeSection(key, config());
    }

    /**
     * Removes a section from the global config file
     * 
     * @param key String in "section" format to set
     * @throws ConfigException if an error is encountered
     */
    @Override
    public void removeSectionGlobal(String key) {
        removeSection(key, globalConfig());
    }

    private static class WiniCache {
        private Wini ini;

        private long lastModified;

        private File configFile;

        public WiniCache(final File configFile) {
            try {
                configFile.createNewFile();
            } catch (IOException e) {
                throw new ConfigException(e, StatusCode.CANNOT_WRITE);
            }
            this.configFile = configFile;
            getWini();
        }

        public synchronized Wini getWini() {
            if (lastModified != configFile.lastModified()) {
                try {
                    this.ini = new Wini(configFile);
                    this.lastModified = configFile.lastModified();
                } catch (Exception e) {
                    throw new ConfigException(e, StatusCode.INVALID_LOCATION);
                }
            }
            return ini;
        }

    }
}
