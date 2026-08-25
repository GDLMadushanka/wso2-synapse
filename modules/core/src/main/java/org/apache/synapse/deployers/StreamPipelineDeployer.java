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
import org.apache.synapse.config.xml.stream.StreamPipelineFactory;
import org.apache.synapse.config.xml.stream.StreamPipelineSerializer;
import org.apache.synapse.stream.pipeline.StreamPipeline;

import java.io.File;
import java.util.Properties;

/**
 * Deploys, updates, undeploys and restores {@code <streamPipeline>} artifacts from
 * {@code synapse-configs/default/stream-pipelines}.
 * <p>
 * This is where {@code validate()} and {@code init()} are called, and therefore where a structurally
 * impossible pipeline is rejected. Doing that here rather than per message is the point: a pipeline
 * whose operators cannot resolve, or which has no way to obtain bytes, fails while someone is
 * watching rather than six hours into a transfer.
 * <p>
 * Note the directory name is <b>plural</b>. {@code MultiXMLConfigurationBuilder} skips a missing
 * directory in silence, so a singular {@code stream-pipeline} folder produces no file, no error and
 * no log line.
 */
public class StreamPipelineDeployer extends AbstractSynapseArtifactDeployer {

    private static final Log log = LogFactory.getLog(StreamPipelineDeployer.class);

    @Override
    public String deploySynapseArtifact(OMElement artifactConfig, String fileName,
                                        Properties properties) {
        if (log.isDebugEnabled()) {
            log.debug("StreamPipeline deployment from file : " + fileName + " : Started");
        }
        try {
            StreamPipeline pipeline = StreamPipelineFactory.createStreamPipeline(artifactConfig,
                    properties);
            if (pipeline == null) {
                handleSynapseArtifactDeploymentError("StreamPipeline deployment failed. The "
                        + "artifact described in the file " + fileName + " is not a StreamPipeline");
                return null;
            }
            pipeline.setFileName(new File(fileName).getName());
            if (log.isDebugEnabled()) {
                log.debug("StreamPipeline named '" + pipeline.getName()
                        + "' has been built from the file " + fileName);
            }

            // validate() already ran inside the factory; init() is what needs an environment.
            pipeline.init(getSynapseEnvironment());
            pipeline.setArtifactContainerName(customLogContent);
            getSynapseConfiguration().addStreamPipeline(pipeline.getName(), pipeline);

            if (log.isDebugEnabled()) {
                log.debug("StreamPipeline deployment from file : " + fileName + " : Completed");
            }
            log.info("StreamPipeline named '" + pipeline.getName() + "' has been deployed from file : "
                    + fileName);
            return pipeline.getName();

        } catch (Exception e) {
            handleSynapseArtifactDeploymentError(
                    "StreamPipeline deployment from the file : " + fileName + " : Failed.", e);
        }
        return null;
    }

    @Override
    public String updateSynapseArtifact(OMElement artifactConfig, String fileName,
                                        String existingArtifactName, Properties properties) {
        if (log.isDebugEnabled()) {
            log.debug("StreamPipeline update from file : " + fileName + " has started");
        }
        try {
            StreamPipeline pipeline = StreamPipelineFactory.createStreamPipeline(artifactConfig,
                    properties);
            if (pipeline == null) {
                handleSynapseArtifactDeploymentError("StreamPipeline update failed. The artifact "
                        + "described in the file " + fileName + " is not a StreamPipeline");
                return null;
            }
            pipeline.setFileName(new File(fileName).getName());
            pipeline.init(getSynapseEnvironment());

            StreamPipeline existing = getSynapseConfiguration().getStreamPipeline(existingArtifactName);

            if (existingArtifactName.equals(pipeline.getName())) {
                getSynapseConfiguration().updateStreamPipeline(existingArtifactName, pipeline);
            } else {
                getSynapseConfiguration().addStreamPipeline(pipeline.getName(), pipeline);
                getSynapseConfiguration().removeStreamPipeline(existingArtifactName);
                log.info("StreamPipeline named '" + existingArtifactName
                        + "' has been undeployed");
            }

            // Destroy the old one only after the new one is registered, so an in-flight transfer
            // finishes against the definition it started with.
            if (existing != null) {
                waitForCompletion();
                existing.destroy();
            }

            log.info("StreamPipeline named '" + pipeline.getName() + "' has been updated from file : "
                    + fileName);
            return pipeline.getName();

        } catch (Exception e) {
            handleSynapseArtifactDeploymentError("Error while updating the StreamPipeline from the "
                    + "file : " + fileName, e);
        }
        return null;
    }

    @Override
    public void undeploySynapseArtifact(String artifactName) {
        if (log.isDebugEnabled()) {
            log.debug("StreamPipeline undeployment of the pipeline named : " + artifactName
                    + " : Started");
        }
        try {
            StreamPipeline pipeline = getSynapseConfiguration().getStreamPipeline(artifactName);
            if (pipeline == null) {
                handleSynapseArtifactDeploymentError("StreamPipeline undeployment failed. No "
                        + "pipeline exists by the name : " + artifactName);
                return;
            }
            getSynapseConfiguration().removeStreamPipeline(artifactName);
            pipeline.destroy();
            log.info("StreamPipeline named '" + artifactName + "' has been undeployed");

        } catch (Exception e) {
            handleSynapseArtifactDeploymentError("StreamPipeline undeployment of pipeline named : "
                    + artifactName + " : Failed", e);
        }
    }

    @Override
    public void restoreSynapseArtifact(String artifactName) {
        if (log.isDebugEnabled()) {
            log.debug("Restoring the StreamPipeline with name : " + artifactName + " : Started");
        }
        try {
            StreamPipeline pipeline = getSynapseConfiguration().getStreamPipeline(artifactName);
            if (pipeline == null) {
                handleSynapseArtifactDeploymentError("Restoring StreamPipeline failed. No pipeline "
                        + "exists by the name : " + artifactName);
                return;
            }
            OMElement pipelineElem = StreamPipelineSerializer.serializeStreamPipeline(null, pipeline);
            if (pipeline.getFileName() != null) {
                String fileName = getServerConfigurationInformation().getSynapseXMLLocation()
                        + File.separator + MultiXMLConfigurationBuilder.STREAM_PIPELINES_DIR
                        + File.separator + pipeline.getFileName();
                writeToFile(pipelineElem, fileName);
                log.info("StreamPipeline named '" + artifactName + "' has been restored");
            } else {
                handleSynapseArtifactDeploymentError("Couldn't restore the StreamPipeline named '"
                        + artifactName + "', filename cannot be found");
            }
        } catch (Exception e) {
            handleSynapseArtifactDeploymentError("Restoring of the StreamPipeline named '"
                    + artifactName + "' has failed", e);
        }
    }
}
