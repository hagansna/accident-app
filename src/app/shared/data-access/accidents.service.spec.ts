import { TestBed } from '@angular/core/testing';

import { AccidentsService } from './accidents.service';

describe('AccidentsService', () => {
  let service: AccidentsService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(AccidentsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
