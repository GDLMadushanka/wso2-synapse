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

package org.apache.synapse.stream;

import java.util.Locale;

/**
 * A byte size written as <b>a number and a unit, in two separate parameters</b>.
 *
 * <pre>
 *   &lt;blockSize&gt;4&lt;/blockSize&gt;
 *   &lt;blockSizeUnit&gt;MB&lt;/blockSizeUnit&gt;
 * </pre>
 *
 * <h2>Why two fields rather than one string</h2>
 * A single {@code 4MB} requires a deployer to know which spellings are accepted before they can type
 * anything, and to get one right with no feedback until deployment. Split, the number is a number and
 * the unit is a closed set a form can offer as a dropdown — the connector's {@code uischema} declares
 * it {@code comboOrExpression}, so VS Code shows the choices rather than a free-text box. A wrong unit
 * becomes unselectable instead of a deployment failure.
 *
 * <h2>The units are binary</h2>
 * {@code KB} is 1024 bytes, {@code MB} is 1024&sup2;, {@code GB} is 1024&sup3; — not the decimal
 * reading. That matches what this feature's sizes always meant, and what the systems underneath use:
 * Azure's own block limits are binary, and so is every buffer here. The {@code KiB}/{@code MiB}/
 * {@code GiB} spellings are accepted for the same values, so a deployer who writes the unambiguous
 * form gets what they expect; they are deliberately <b>not</b> offered in the dropdown, because two
 * entries meaning one thing is a worse form than one.
 *
 * <p>This is a utility rather than a contract: it holds no state, and an operator that wants to size
 * something in bytes is free to ignore it. It lives here so that one unit table serves every connector
 * instead of four copies drifting apart.
 */
public final class ByteSize {

    private ByteSize() {
    }

    /** The units a form should offer, in the order a form should offer them. */
    public static final String[] UNITS = {"B", "KB", "MB", "GB"};

    /**
     * Resolves an amount and a unit to bytes.
     *
     * @param amount    the number, as written; {@code null} or blank means the caller's default applies
     * @param unit      the unit, case-insensitive: B, KB, MB, GB, KiB, MiB, GiB. Blank means bytes
     * @param operator  the operator's name, for the message
     * @param parameter the amount parameter's name, so the message names the field to fix
     * @return the size in bytes, or {@code null} when {@code amount} was absent
     * @throws StreamException if the amount is not a whole number, the unit is not one of the above,
     *                         the amount carries a unit suffix of its own, or the result is below one
     *                         byte or overflows
     */
    public static Long bytes(String amount, String unit, String operator, String parameter)
            throws StreamException {

        String number = amount == null ? null : amount.trim();
        if (number == null || number.isEmpty()) {
            return null;
        }

        // Caught explicitly rather than failing as "not a number", because "4MB" is exactly what
        // someone who has not seen the unit field will type, and the fix is to move it rather than
        // to remove it.
        if (!number.chars().allMatch(Character::isDigit)) {
            throw new StreamException(operator + " has " + parameter + "=\"" + amount + "\"; this is"
                    + " the number alone, with the unit in '" + parameter + "Unit'. Write"
                    + " <" + parameter + ">4</" + parameter + "> and"
                    + " <" + parameter + "Unit>MB</" + parameter + "Unit>", false);
        }

        long multiplier = multiplierFor(unit, operator, parameter);
        long value;
        try {
            value = Math.multiplyExact(Long.parseLong(number), multiplier);
        } catch (ArithmeticException tooBig) {
            throw new StreamException(operator + " has " + parameter + "=\"" + amount + "\" "
                    + (unit == null || unit.isBlank() ? "bytes" : unit.trim())
                    + ", which is larger than any size this can hold", tooBig, false);
        }
        if (value < 1L) {
            throw new StreamException(operator + " has " + parameter + "=\"" + amount + "\"; it must"
                    + " be at least one byte", false);
        }
        return value;
    }

    private static long multiplierFor(String unit, String operator, String parameter)
            throws StreamException {
        String u = unit == null ? "" : unit.trim().toUpperCase(Locale.ROOT);
        switch (u) {
            case "":
            case "B":
                return 1L;
            case "KB":
            case "KIB":
                return 1024L;
            case "MB":
            case "MIB":
                return 1024L * 1024;
            case "GB":
            case "GIB":
                return 1024L * 1024 * 1024;
            default:
                throw new StreamException(operator + " has " + parameter + "Unit=\"" + unit + "\";"
                        + " expected one of B, KB, MB, GB", false);
        }
    }
}
