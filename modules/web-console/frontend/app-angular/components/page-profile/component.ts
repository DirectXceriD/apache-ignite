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

import {merge, combineLatest} from 'rxjs';
import {tap, pairwise, startWith, distinctUntilChanged, pluck} from 'rxjs/operators';
import {Component, Inject, OnInit, OnDestroy, ViewChild, ElementRef} from '@angular/core';
import {FormGroup, FormControl, Validators, Validator, AbstractControl} from '@angular/forms';
import templateUrl from 'file-loader!./template.html';
import './style.scss';
import {default as CountriesFactory, Country} from 'app/services/Countries.service';
import {default as UserFactory, User} from 'app/modules/user/User.service';
import {Confirm} from 'app/services/Confirm.service';
import {default as LegacyUtilsFactory} from 'app/services/LegacyUtils.service';
import {
    FORM_FIELD_OPTIONS, FormFieldRequiredMarkerStyles, FormFieldErrorStyles
} from '../formField.component';

@Component({
    selector: 'page-profile-angular',
    templateUrl,
    viewProviders: [
        {
            provide: FORM_FIELD_OPTIONS,
            useValue: {
                requiredMarkerStyle: FormFieldRequiredMarkerStyles.OPTIONAL,
                // requiredMarkerStyle: FormFieldRequiredMarkerStyles.REQUIRED,
                errorStyle: FormFieldErrorStyles.ICON
                // errorStyle: FormFieldErrorStyles.INLINE
            }
        }
    ]
})
export class PageProfile implements OnInit, OnDestroy {
    static parameters = [
        [new Inject('IgniteCountries')],
        [new Inject('User')],
        [new Inject('Confirm')],
        [new Inject('IgniteLegacyUtils')]
    ]
    constructor(
        Countries: ReturnType<typeof CountriesFactory>,
        private User: ReturnType<typeof UserFactory>,
        private Confirm: Confirm,
        private LegacyUtils: ReturnType<typeof LegacyUtilsFactory>
    ) {
        this.countries = Countries.getAll();
    }

    countries: Country[]
    user: User

    @ViewChild('passwordEl')
    set passwordEl(el: ElementRef<HTMLInputElement>) {
        if (el) el.nativeElement.focus();
    }


    async ngOnInit() {
        this.user = await this.User.read();
        this.form.patchValue(this.user);
    }
    ngOnDestroy() {
        this.subscriber.unsubscribe();
    }
    saveUser() {
        console.log(this.form.getRawValue());
    }
    async generateToken() {
        try {
            await this.Confirm.confirm('Are you sure you want to change security token?<br>If you change the token you will need to restart the agent.');
            this.form.get('token').setValue(this.LegacyUtils.randomString(20));
        } catch (e) {
            // no-op
        }
    }

    form = new FormGroup({
        firstName: new FormControl('', [Validators.required]),
        lastName: new FormControl('', [Validators.required]),
        email: new FormControl('', [Validators.required, Validators.email]),
        phone: new FormControl('', []),
        country: new FormControl('', [Validators.required]),
        company: new FormControl('', [Validators.required]),
        passwordPanelOpened: new FormControl(false, []),
        newPassword: new FormControl('', []),
        confirmPassword: new FormControl('', []),
        token: new FormControl({value: '', disabled: true}, [Validators.required])
    })
    subscriber = merge(
        combineLatest(
            this.form.get('passwordPanelOpened').valueChanges.pipe(startWith(this.form.get('passwordPanelOpened').value)),
            this.form.get('newPassword').valueChanges.pipe(startWith(this.form.get('newPassword').value))
        ).pipe(
            tap(([panelOpened, newPassword]) => {
                const requiredIfOpen = (c) => !panelOpened ? null : Validators.required(c);
                const passwordMatch = (c) => newPassword === c.value ? null : {passwordMatch: true};
                this.form.get('newPassword').setValidators([requiredIfOpen]);
                this.form.get('newPassword').updateValueAndValidity({emitEvent: false});
                this.form.get('confirmPassword').setValidators([requiredIfOpen, passwordMatch]);
                this.form.get('confirmPassword').updateValueAndValidity();
            })
        )
    ).subscribe();
}
