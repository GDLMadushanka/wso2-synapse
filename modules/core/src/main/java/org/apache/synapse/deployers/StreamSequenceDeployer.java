/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.synapse.deployers;

import org.apache.axiom.om.OMElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.config.xml.MultiXMLConfigurationBuilder;
import org.apache.synapse.config.xml.stream.StreamSequenceFactory;
import org.apache.synapse.config.xml.stream.StreamSequenceSerializer;
import org.apache.synapse.stream.sequence.StreamSequence;

import java.io.File;
import java.util.Properties;

/**
 * Deploys, updates, undeploys and restores {@code <streamSequence>} artifacts from
 * {@code synapse-configs/default/stream-sequences}.
 * <p>
 * This is where {@code validate()} and {@code init()} are called, and therefore where a structurally
 * impossible sequence is rejected. Doing that here rather than per message is the point: a sequence
 * whose operators cannot resolve, or which has no way to obtain bytes, fails while someone is
 * watching rather than six hours into a transfer.
 * <p>
 * Note the directory name is <b>plural</b>. {@code MultiXMLConfigurationBuilder} skips a missing
 * directory in silence, so a singular {@code stream-sequence} folder produces no file, no error and
 * no log line.
 */
public class StreamSequenceDeployer extends AbstractSynapseArtifactDeployer {

    private static final Log log = LogFactory.getLog(StreamSequenceDeployer.class);

    @Override
    public String deploySynapseArtifact(OMElement artifactConfig, String fileName,
                                        Properties properties) {
        if (log.isDebugEnabled()) {
            log.debug("StreamSequence deployment from file : " + fileName + " : Started");
        }
        try {
            StreamSequence sequence = StreamSequenceFactory.createStreamSequence(artifactConfig,
                    properties);
            if (sequence == null) {
                handleSynapseArtifactDeploymentError("StreamSequence deployment failed. The "
                        + "artifact described in the file " + fileName + " is not a StreamSequence");
                return null;
            }
            sequence.setFileName(new File(fileName).getName());
            if (log.isDebugEnabled()) {
                log.debug("StreamSequence named '" + sequence.getName()
                        + "' has been built from the file " + fileName);
            }

            // validate() already ran inside the factory; init() is what needs an environment.
            sequence.init(getSynapseEnvironment());
            sequence.setArtifactContainerName(customLogContent);
            getSynapseConfiguration().addStreamSequence(sequence.getName(), sequence);

            if (log.isDebugEnabled()) {
                log.debug("StreamSequence deployment from file : " + fileName + " : Completed");
            }
            log.info("StreamSequence named '" + sequence.getName() + "' has been deployed from file : "
                    + fileName);
            return sequence.getName();

        } catch (Exception e) {
            handleSynapseArtifactDeploymentError(
                    "StreamSequence deployment from the file : " + fileName + " : Failed.", e);
        }
        return null;
    }

    @Override
    public String updateSynapseArtifact(OMElement artifactConfig, String fileName,
                                        String existingArtifactName, Properties properties) {
        if (log.isDebugEnabled()) {
            log.debug("StreamSequence update from file : " + fileName + " has started");
        }
        try {
            StreamSequence sequence = StreamSequenceFactory.createStreamSequence(artifactConfig,
                    properties);
            if (sequence == null) {
                handleSynapseArtifactDeploymentError("StreamSequence update failed. The artifact "
                        + "described in the file " + fileName + " is not a StreamSequence");
                return null;
            }
            sequence.setFileName(new File(fileName).getName());
            sequence.init(getSynapseEnvironment());

            StreamSequence existing = getSynapseConfiguration().getStreamSequence(existingArtifactName);

            if (existingArtifactName.equals(sequence.getName())) {
                getSynapseConfiguration().updateStreamSequence(existingArtifactName, sequence);
            } else {
                getSynapseConfiguration().addStreamSequence(sequence.getName(), sequence);
                getSynapseConfiguration().removeStreamSequence(existingArtifactName);
                log.info("StreamSequence named '" + existingArtifactName
                        + "' has been undeployed");
            }

            // Destroy the old one only after the new one is registered, so an in-flight transfer
            // finishes against the definition it started with.
            if (existing != null) {
                waitForCompletion();
                existing.destroy();
            }

            log.info("StreamSequence named '" + sequence.getName() + "' has been updated from file : "
                    + fileName);
            return sequence.getName();

        } catch (Exception e) {
            handleSynapseArtifactDeploymentError("Error while updating the StreamSequence from the "
                    + "file : " + fileName, e);
        }
        return null;
    }

    @Override
    public void undeploySynapseArtifact(String artifactName) {
        if (log.isDebugEnabled()) {
            log.debug("StreamSequence undeployment of the sequence named : " + artifactName
                    + " : Started");
        }
        try {
            StreamSequence sequence = getSynapseConfiguration().getStreamSequence(artifactName);
            if (sequence == null) {
                handleSynapseArtifactDeploymentError("StreamSequence undeployment failed. No "
                        + "sequence exists by the name : " + artifactName);
                return;
            }
            getSynapseConfiguration().removeStreamSequence(artifactName);
            sequence.destroy();
            log.info("StreamSequence named '" + artifactName + "' has been undeployed");

        } catch (Exception e) {
            handleSynapseArtifactDeploymentError("StreamSequence undeployment of sequence named : "
                    + artifactName + " : Failed", e);
        }
    }

    @Override
    public void restoreSynapseArtifact(String artifactName) {
        if (log.isDebugEnabled()) {
            log.debug("Restoring the StreamSequence with name : " + artifactName + " : Started");
        }
        try {
            StreamSequence sequence = getSynapseConfiguration().getStreamSequence(artifactName);
            if (sequence == null) {
                handleSynapseArtifactDeploymentError("Restoring StreamSequence failed. No sequence "
                        + "exists by the name : " + artifactName);
                return;
            }
            OMElement sequenceElem = StreamSequenceSerializer.serializeStreamSequence(null, sequence);
            if (sequence.getFileName() != null) {
                String fileName = getServerConfigurationInformation().getSynapseXMLLocation()
                        + File.separator + MultiXMLConfigurationBuilder.STREAM_SEQUENCES_DIR
                        + File.separator + sequence.getFileName();
                writeToFile(sequenceElem, fileName);
                log.info("StreamSequence named '" + artifactName + "' has been restored");
            } else {
                handleSynapseArtifactDeploymentError("Couldn't restore the StreamSequence named '"
                        + artifactName + "', filename cannot be found");
            }
        } catch (Exception e) {
            handleSynapseArtifactDeploymentError("Restoring of the StreamSequence named '"
                    + artifactName + "' has failed", e);
        }
    }
}
