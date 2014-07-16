/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.web.api;

import org.locationtech.geogig.api.GeoGIT;

/**
 *
 */
public interface CommandContext {

    /**
     * @return the {@link GeoGIT} for this context.
     */
    GeoGIT getGeoGIT();

    /**
     * Sets the response for the context.
     * 
     * @param responseContent the command response
     */
    void setResponseContent(CommandResponse responseContent);

    /**
     * Sets the response for the context.
     * 
     * @param responseContent the command response
     */
    void setResponseContent(StreamResponse responseContent);

}