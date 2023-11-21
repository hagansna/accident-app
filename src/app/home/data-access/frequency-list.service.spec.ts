import { TestBed } from '@angular/core/testing';

import { FrequencyListService } from './frequency-list.service';

describe('FrequencyListService', () => {
  let service: FrequencyListService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(FrequencyListService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
