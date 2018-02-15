/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql;

import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Parses SQL escape sequences and converts them to string. */
public class SqlEscapeSeqParser {
    /** Sentinel value for the case of single character escape sequence (like {@code \n} or {@code \t}). */
    private static final int SINGLE_CHAR_RADIX = -1;

    /** Current parser state. */
    public enum State {
        /**
         * Just started, haven't read even a single value character.
         * Note that the escape start character is already parsed by this time. */
        START,
        /** In the middle of processing of escape sequence. */
        PROCESSING,
        /** Escape sequence finished, the last character was accepted. */
        FINISHED_CHAR_ACCEPTED,
        /** Escape sequence finished, the last character was rejected. */
        FINISHED_CHAR_REJECTED,
        /** Error in the escape sequence has encountered. */
        ERROR
    }

    /** Parser state. */
    private State state;

    /** Minimal length of the sequence. */
    private int minLen;

    /** Maximal length of the sequence. */
    private int maxLen;

    /** Radix of character number. Can be also {@link #SINGLE_CHAR_RADIX}. */
    private int radix;

    /** Parser input sequence. */
    private final StringBuffer input = new StringBuffer();

    /**
     * Creates a parser in {@link State#START} state.
     *
     * <p>Note that the escape start character is already parsed by this time.
     */
    public SqlEscapeSeqParser() {
        state = State.START;
    }

    /**
     * Feeds a next character to the parser.
     *
     * @param c The character.
     * @return The new state of the parser. See {@link State} for details.
     */
    public State accept(char c) {
        if (state == State.START)
            acceptPrefix(c);
        else
            acceptValueChar(c);

        return state;
    }

    /**
     * Processes the first character of the escape sequence and defines min/max length, the radix and so on.
     *
     * @param c The character.
     */
    private void acceptPrefix(char c) {
        if (state != State.START)
            throw invalidStateException();

        state = State.PROCESSING;

        switch (c) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
                minLen = 1;
                maxLen = 3;
                radix = 8;
                acceptValueChar(c);
                break;

            case 'x':
            case 'X':
                minLen = 1;
                maxLen = 2;
                radix = 16;
                break;

            // Upper-case 'U' is intentionally not supported (as in Postgres),
            // because in Java we don't support anything beyond Basic Multilingual Plane
            // because characters above it (with codepoints >= 0x10000, supplementary ones, surrogates)
            // are complex to handle. We still allow the surrogates.
            case 'u':
                minLen = 4;
                maxLen = 4;
                radix = 16;
                break;

            default:
                minLen = 1;
                maxLen = 1;
                radix = SINGLE_CHAR_RADIX;
                acceptValueChar(c);
        }
    }

    /**
     * Processes the character starting from the second one.
     *
     * @param c The character.
     */
    private void acceptValueChar(char c) {
        int inputLen = input.length();

        if (state == State.PROCESSING && radix != SINGLE_CHAR_RADIX && !isValidDigit(c)) {
            state = inputLen >= minLen && isValidUnicodeInput() ? State.FINISHED_CHAR_REJECTED : State.ERROR;

            return;
        }

        if (inputLen >= maxLen || !isValidInput(c)) {
            state = isValidUnicodeInput() ? State.FINISHED_CHAR_REJECTED : State.ERROR;

            return;
        }

        input.append(c);

        if (input.length() >= maxLen)
            state = isValidUnicodeInput() ? State.FINISHED_CHAR_ACCEPTED : State.ERROR;
    }

    /**
     * Tells the parser that end of input has occured.
     *
     * @return The new state of the parser. See {@link State} for details.
     */
    public State acceptEnd() {
        int inputLen = input.length();

        switch (state) {
            case FINISHED_CHAR_ACCEPTED:
            case FINISHED_CHAR_REJECTED:
            case ERROR:
                break;

            case START:
                state = State.ERROR;

                break;

            case PROCESSING:
                if (radix != SINGLE_CHAR_RADIX) {
                    assert inputLen <= maxLen;

                    state = inputLen >= minLen && isValidUnicodeInput() ? State.FINISHED_CHAR_ACCEPTED : State.ERROR;

                    return state;
                }

                break;

            default:
                throw invalidStateException();
        }

        return state;
    }

    /** Checks if the character is a valid digit in the current {@link #radix}. */
    private boolean isValidDigit(char c) {
        if (radix < 10)
            return c >= '0' && c < ('0' + radix);
        else
            return (c >= '0' && c <= '9') ||
                (c >= 'a' && c < 'a' + (radix - 10)) ||
                (c >= 'A' && c < 'A' + (radix - 10));
    }

    /**
     * Checks if {@link #input} plus the current character constitutes a valid input.
     *
     * <p>Currently is used to silently reject octal characters \400..\777</p>.
     *
     * @param c The character being processed
     * @return true if the input is valid, false if not and the processing of escape sequence should cease before
     * the current character.
     */
    private boolean isValidInput(char c) {
        if (radix == 8 && input.length() == 2 && input.charAt(0) >= '4')
            return false;

        return true;
    }

    /**
     * Checks if {@link #input} represents a valid Unicode codepoint.
     *
     * @return true if the input is valid Unicode codepoint, false if not.
     */
    private boolean isValidUnicodeInput() {
        if (radix != 16)
            return true;

        int codePnt = Integer.parseInt(input.toString(), radix);

        return Character.isValidCodePoint(codePnt);
    }

    /**
     * Returns the state.
     *
     * @return The state.
     */
    public State state() {
        return state;
    }

    /**
     * Converts the input to the string it encodes.
     *
     * @return The string decoded from the escape sequence.
     */
    public String convertedStr() {
        if (state != State.FINISHED_CHAR_ACCEPTED && state != State.FINISHED_CHAR_REJECTED)
            throw invalidStateException();

        if (radix == SINGLE_CHAR_RADIX)
            return Character.toString(convertEscSeqChar(input.charAt(0)));
        else
            return new String(Character.toChars(Integer.parseInt(input.toString(), radix)));
    }

    /**
     * Converts the second character from one-character escape sequence to the actual character.
     *
     * @param c The character after the backquote.
     * @return the character which this escape sequence represents.
     */
    private static char convertEscSeqChar(char c) {
        switch (c) {
            case 'b':
                return '\b';
            case 'f':
                return '\f';
            case 'n':
                return '\n';
            case 'r':
                return '\r';
            case 't':
                return '\t';
            case 'Z':
                return '\032';

            default:
                return c;
        }
    }

    /**
     * Replaces all occurences of escape sequences in the string and returns the result.
     *
     * @param input The input string.
     * @param escapeChars Escape sequence start characters.
     * @param errorReporter Error reporter function or null to silently drop invalid escape sequences.
     *      It may throw an unchecked exception to terminate processing.
     *      The error function accepts the invalid escape sequence substring and its position in the input.
     * @return The string with escape sequences replaced or null if input was null.
     * @throws RuntimeException Any exception that errorReporter might throw.
     */
    @Nullable public static String replaceAll(@Nullable String input, String escapeChars,
        IgniteBiInClosure<String, Integer> errorReporter) {
        if (F.isEmpty(input))
            return input;

        StringBuilder output = new StringBuilder(input.length());

        int lastCopiedPos = 0;

        for (int curPos = 0; curPos < input.length(); curPos++) {
            if (escapeChars.indexOf(input.charAt(curPos)) == -1)
                continue;

            output.append(input, lastCopiedPos, curPos);
            lastCopiedPos = curPos;

            SqlEscapeSeqParser seqParser = new SqlEscapeSeqParser();

            do {
                curPos++;

                if (curPos >= input.length()) {
                    seqParser.acceptEnd();

                    break;
                }
            } while (seqParser.accept(input.charAt(curPos)) == State.PROCESSING);

            switch (seqParser.state()) {
                case ERROR:
                    if (errorReporter != null)
                        errorReporter.apply("Can't parse input '" + input.substring(lastCopiedPos, curPos) + "'",
                            lastCopiedPos);

                    break;

                case FINISHED_CHAR_REJECTED:
                    curPos--;

                    // FALL THROUGH

                case FINISHED_CHAR_ACCEPTED:
                    output.append(seqParser.convertedStr());

                    break;

                default:
                    throw invalidStateException();
            }

            lastCopiedPos = curPos;
        }

        output.append(input, lastCopiedPos, input.length());

        return output.toString();
    }

    /**
     * Create invalid escape sequence parser state exception.
     *
     * @return The invalid escape sequence parser state exception.
     */
    @NotNull private static IgniteIllegalStateException invalidStateException() {
        return new IgniteIllegalStateException("Invalid escape sequence processor state");
    }
}
